// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "google/cloud/storage/internal/hedged_object_read_source.h"
#include "google/cloud/internal/make_status.h"
#include <atomic>
#include <cstring>
#include <future>
#include <mutex>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {
namespace {

struct RaceResult {
  StatusOr<ReadSourceResult> result;
  std::unique_ptr<ObjectReadSource> source;
  std::unique_ptr<char[]> buffer;
};

struct AttemptHandle {
  std::shared_ptr<std::atomic<bool>> cancel_token =
      std::make_shared<std::atomic<bool>>(false);
  std::mutex mu;
  ObjectReadSource* source = nullptr;
};

}  // namespace

struct RaceState {
  std::promise<RaceResult> promise;
  std::atomic<bool> resolved{false};
  std::mutex mu;
  std::vector<std::shared_ptr<AttemptHandle>> attempts;

  void CancelAllExcept(AttemptHandle const* winner_handle) {
    std::vector<std::shared_ptr<AttemptHandle>> copy;
    {
      std::lock_guard<std::mutex> lk(mu);
      copy = attempts;
    }
    for (auto const& attempt : copy) {
      if (attempt.get() == winner_handle) continue;
      attempt->cancel_token->store(true, std::memory_order_relaxed);
      std::lock_guard<std::mutex> lk(attempt->mu);
      if (attempt->source != nullptr) {
        attempt->source->Cancel();
      }
    }
  }

  void CancelAll() {
    CancelAllExcept(nullptr);
  }

  // Resolve the race with @p status (unless an attempt already resolved it)
  // and cancel every attempt. Attempts do not fulfill the promise when they
  // observe a cancelled token, so an external Cancel() or Close() must
  // resolve the race itself or the reader blocks in `future.get()` forever.
  void Abort(Status status) {
    bool expected = false;
    if (resolved.compare_exchange_strong(expected, true)) {
      promise.set_value(RaceResult{std::move(status), nullptr, {}});
    }
    CancelAll();
  }
};

namespace {

// Opens a new child and performs its initial read, resolving the race if this
// attempt finishes first. Losing attempts are cancelled and closed. Only the
// primary attempt resolves the race on an open or read error: a hedge that fails
// to open or read must not mask a slower, but successful, primary.
void RunAttempt(std::shared_ptr<RaceState> const& state,
                std::shared_ptr<AttemptHandle> const& attempt_handle,
                HedgedObjectReadSource::ChildFactory const& factory,
                std::size_t n, bool resolve_on_error,
                std::shared_ptr<HedgingThreadPool> release_slot) {
  struct SlotGuard {
    std::shared_ptr<HedgingThreadPool> pool;
    ~SlotGuard() {
      if (pool) pool->ReleaseHedgeSlot();
    }
  } guard{std::move(release_slot)};

  if (state->resolved.load(std::memory_order_relaxed) ||
      attempt_handle->cancel_token->load(std::memory_order_relaxed)) {
    return;
  }

  StatusOr<std::unique_ptr<ObjectReadSource>> source =
      factory(attempt_handle->cancel_token);
  if (attempt_handle->cancel_token->load(std::memory_order_relaxed) ||
      state->resolved.load(std::memory_order_relaxed)) {
    if (source) {
      (*source)->Cancel();
      (void)(*source)->Close();
    }
    return;
  }

  if (!source) {
    if (!resolve_on_error) return;
    bool expected = false;
    if (state->resolved.compare_exchange_strong(expected, true)) {
      state->CancelAllExcept(attempt_handle.get());
      state->promise.set_value(
          RaceResult{std::move(source).status(), nullptr, {}});
    }
    return;
  }
  std::unique_ptr<char[]> buffer(new (std::nothrow) char[n]);
  if (!buffer) {
    if (!resolve_on_error) return;
    bool expected = false;
    if (state->resolved.compare_exchange_strong(expected, true)) {
      state->CancelAllExcept(attempt_handle.get());
      state->promise.set_value(RaceResult{
          google::cloud::internal::ResourceExhaustedError(
              "Out of memory allocating hedge buffer", GCP_ERROR_INFO()),
          nullptr,
          {}});
    }
    return;
  }

  {
    std::lock_guard<std::mutex> lk(attempt_handle->mu);
    attempt_handle->source = source->get();
  }

  // A `CancelAllExcept()` running between the token check above and the
  // registration of the source saw a null `source` and could not cancel it.
  // Re-check the token now that the source is visible, or this attempt would
  // proceed into a blocking `Read()` that nothing can interrupt.
  if (attempt_handle->cancel_token->load(std::memory_order_relaxed) ||
      state->resolved.load(std::memory_order_relaxed)) {
    {
      std::lock_guard<std::mutex> lk(attempt_handle->mu);
      attempt_handle->source = nullptr;
    }
    (*source)->Cancel();
    (void)(*source)->Close();
    return;
  }

  StatusOr<ReadSourceResult> result = (*source)->Read(buffer.get(), n);

  {
    std::lock_guard<std::mutex> lk(attempt_handle->mu);
    attempt_handle->source = nullptr;
  }

  if (!result && !resolve_on_error) {
    (void)(*source)->Close();
    return;
  }

  bool expected = false;
  if (state->resolved.compare_exchange_strong(expected, true)) {
    state->CancelAllExcept(attempt_handle.get());
    state->promise.set_value(
        RaceResult{std::move(result), *std::move(source), std::move(buffer)});
  } else {
    (void)(*source)->Close();
  }
}

}  // namespace

HedgedObjectReadSource::HedgedObjectReadSource(
    std::shared_ptr<HedgingThreadPool> hedge_pool, ChildFactory child_factory,
    std::chrono::milliseconds delay, int max_hedges, std::size_t max_buffer)
    : hedge_pool_(std::move(hedge_pool)),
      child_factory_(std::move(child_factory)),
      delay_(delay),
      max_hedges_(max_hedges),
      max_buffer_(max_buffer) {}

HedgedObjectReadSource::HedgedObjectReadSource(
    std::shared_ptr<HedgingThreadPool> hedge_pool,
    SimpleChildFactory child_factory, std::chrono::milliseconds delay,
    int max_hedges, std::size_t max_buffer)
    : HedgedObjectReadSource(
          std::move(hedge_pool),
          [simple = std::move(child_factory)](
              std::shared_ptr<std::atomic<bool>> const&) { return simple(); },
          delay, max_hedges, max_buffer) {}

HedgedObjectReadSource::~HedgedObjectReadSource() = default;

bool HedgedObjectReadSource::IsOpen() const {
  std::lock_guard<std::mutex> lk(mu_);
  if (active_child_) return active_child_->IsOpen();
  return !is_closed_;
}

StatusOr<HttpResponse> HedgedObjectReadSource::Close() {
  std::shared_ptr<RaceState> race;
  std::unique_ptr<ObjectReadSource> child;
  {
    std::lock_guard<std::mutex> lk(mu_);
    is_closed_ = true;
    race = current_race_;
    if (reading_) {
      // A Read() is in flight on `active_child_`: destroying the child here
      // would leave the reader executing inside a deleted object. Unblock the
      // reader instead; it discards the child when it returns.
      if (active_child_) active_child_->Cancel();
    } else {
      child = std::move(active_child_);
    }
  }
  if (race) {
    race->Abort(google::cloud::internal::CancelledError(
        "Stream closed while a read was in progress", GCP_ERROR_INFO()));
  }
  if (child) return child->Close();
  // The source was never read from, there is no child (or HTTP response) to
  // close.
  return HttpResponse{HttpStatusCode::kOk, {}, {}};
}

void HedgedObjectReadSource::Cancel() {
  std::shared_ptr<RaceState> race;
  {
    std::lock_guard<std::mutex> lk(mu_);
    is_cancelled_ = true;
    race = current_race_;
    if (active_child_) {
      active_child_->Cancel();
    }
  }
  if (race) {
    race->Abort(google::cloud::internal::CancelledError("Request cancelled",
                                                        GCP_ERROR_INFO()));
  }
}

StatusOr<ReadSourceResult> HedgedObjectReadSource::Read(char* buf,
                                                        std::size_t n) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (is_closed_) return ReadSourceResult{};
    if (is_cancelled_) {
      return google::cloud::internal::CancelledError("Request cancelled",
                                                     GCP_ERROR_INFO());
    }
    reading_ = true;
  }
  auto result = ReadImpl(buf, n);
  // While `reading_` was set, Close() deferred the destruction of
  // `active_child_` to this thread: it cannot delete an object this thread
  // may still be executing in. Perform any deferred cleanup now.
  std::unique_ptr<ObjectReadSource> discard;
  bool cancelled = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    reading_ = false;
    if (is_closed_ || is_cancelled_) {
      discard = std::move(active_child_);
      cancelled = is_cancelled_;
    }
  }
  if (discard) {
    if (cancelled) discard->Cancel();
    (void)discard->Close();
  }
  return result;
}

StatusOr<ReadSourceResult> HedgedObjectReadSource::ReadImpl(char* buf,
                                                            std::size_t n) {
  ObjectReadSource* child = nullptr;
  {
    std::lock_guard<std::mutex> lk(mu_);
    // Only the stream open is hedged. Once a child has won the race all
    // subsequent reads continue on it, at its current offset, without any
    // thread hops or extra copies.
    if (active_child_) child = active_child_.get();
  }
  // `child` remains valid outside the lock: Close() does not destroy the
  // child while `reading_` is set, and this thread only destroys it after
  // ReadImpl() returns.
  if (child != nullptr) return child->Read(buf, n);

  // Racing requires one staging buffer of `n` bytes per attempt, on top of the
  // caller's own buffer. For a large read that multiplication is worse than
  // the tail latency it avoids, so open the stream without hedging and read
  // straight into the caller's buffer.
  if (n > max_buffer_) {
    auto cancel_token = std::make_shared<std::atomic<bool>>(false);
    StatusOr<std::unique_ptr<ObjectReadSource>> new_child =
        child_factory_(cancel_token);
    if (!new_child) return std::move(new_child).status();
    ObjectReadSource* raw = new_child->get();
    bool installed = false;
    bool cancelled = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      cancelled = is_cancelled_;
      if (!is_cancelled_ && !is_closed_) {
        active_child_ = *std::move(new_child);
        installed = true;
      }
    }
    if (!installed) {
      // A Cancel() or Close() raced with the open.
      (*new_child)->Cancel();
      (void)(*new_child)->Close();
      if (cancelled) {
        return google::cloud::internal::CancelledError("Request cancelled",
                                                       GCP_ERROR_INFO());
      }
      return ReadSourceResult{};
    }
    return raw->Read(buf, n);
  }

  auto state = std::make_shared<RaceState>();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (is_cancelled_ || is_closed_) {
      return google::cloud::internal::CancelledError("Request cancelled",
                                                     GCP_ERROR_INFO());
    }
    current_race_ = state;
  }
  std::future<RaceResult> future = state->promise.get_future();

  auto primary_handle = std::make_shared<AttemptHandle>();
  {
    std::lock_guard<std::mutex> lk(state->mu);
    state->attempts.push_back(primary_handle);
  }

  auto primary = [state, primary_handle, factory = child_factory_, n] {
    RunAttempt(state, primary_handle, factory, n,
               /*resolve_on_error=*/true, nullptr);
  };
  // If the pool is shutting down run the attempt inline, the read must
  // complete either way.
  if (!hedge_pool_->Enqueue(primary)) primary();

  for (int i = 0; i != max_hedges_; ++i) {
    if (future.wait_for(delay_) != std::future_status::timeout) break;
    if (!hedge_pool_->TryAcquireHedgeToken()) continue;
    auto hedge_handle = std::make_shared<AttemptHandle>();
    {
      std::lock_guard<std::mutex> lk(state->mu);
      state->attempts.push_back(hedge_handle);
    }
    auto hedge = [state, hedge_handle, factory = child_factory_, n,
                  pool = hedge_pool_] {
      RunAttempt(state, hedge_handle, factory, n,
                 /*resolve_on_error=*/false, pool);
    };
    if (!hedge_pool_->Enqueue(hedge)) {
      hedge_pool_->ReleaseHedgeSlot();
      break;
    }
  }

  RaceResult race = future.get();
  bool closed = false;
  bool cancelled = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    current_race_.reset();
    closed = is_closed_;
    cancelled = is_cancelled_;
    // Do not install the winner when a Close() or Cancel() raced with the
    // attempts: the caller's Close() already returned and nothing would ever
    // close the child.
    if (!closed && !cancelled) active_child_ = std::move(race.source);
  }
  if (closed || cancelled) {
    if (race.source) {
      race.source->Cancel();
      (void)race.source->Close();
    }
    if (cancelled) {
      return google::cloud::internal::CancelledError("Request cancelled",
                                                     GCP_ERROR_INFO());
    }
    return ReadSourceResult{};
  }
  if (race.result.ok() && race.result->bytes_received > 0) {
    std::memcpy(buf, race.buffer.get(), race.result->bytes_received);
  }
  return race.result;
}

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
