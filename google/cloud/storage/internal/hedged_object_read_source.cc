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
  {
    std::lock_guard<std::mutex> lk(mu_);
    is_closed_ = true;
    race = current_race_;
    if (active_child_) return active_child_->Close();
  }
  if (race) {
    race->CancelAll();
  }
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
    race->CancelAll();
  }
}

StatusOr<ReadSourceResult> HedgedObjectReadSource::Read(char* buf,
                                                        std::size_t n) {
  if (is_closed_) return ReadSourceResult{};

  {
    std::lock_guard<std::mutex> lk(mu_);
    if (is_cancelled_) {
      return google::cloud::internal::CancelledError("Request cancelled",
                                                     GCP_ERROR_INFO());
    }
    // Only the stream open is hedged. Once a child has won the race all
    // subsequent reads continue on it, at its current offset, without any
    // thread hops or extra copies.
    if (active_child_) return active_child_->Read(buf, n);
  }

  // Racing requires one staging buffer of `n` bytes per attempt, on top of the
  // caller's own buffer. For a large read that multiplication is worse than
  // the tail latency it avoids, so open the stream without hedging and read
  // straight into the caller's buffer.
  if (n > max_buffer_) {
    auto cancel_token = std::make_shared<std::atomic<bool>>(false);
    StatusOr<std::unique_ptr<ObjectReadSource>> child =
        child_factory_(cancel_token);
    if (!child) return std::move(child).status();
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (is_cancelled_) {
        (*child)->Cancel();
        (void)(*child)->Close();
        return google::cloud::internal::CancelledError("Request cancelled",
                                                       GCP_ERROR_INFO());
      }
      active_child_ = *std::move(child);
    }
    return active_child_->Read(buf, n);
  }

  auto state = std::make_shared<RaceState>();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (is_cancelled_) {
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
  {
    std::lock_guard<std::mutex> lk(mu_);
    current_race_.reset();
    active_child_ = std::move(race.source);
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
