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

struct RaceState {
  std::promise<RaceResult> promise;
  std::atomic<bool> resolved{false};
  std::atomic<int> active_attempts{0};
  std::mutex mu;
  Status last_error;
};

void ResolveOnError(std::shared_ptr<RaceState> const& state, Status status) {
  {
    std::lock_guard<std::mutex> lock(state->mu);
    state->last_error = std::move(status);
  }
  if (state->active_attempts.fetch_sub(1) == 1) {
    bool expected = false;
    if (state->resolved.compare_exchange_strong(expected, true)) {
      Status error;
      {
        std::lock_guard<std::mutex> lock(state->mu);
        error = std::move(state->last_error);
      }
      state->promise.set_value(RaceResult{std::move(error), nullptr, nullptr});
    }
  }
}

void ResolveOnSuccess(std::shared_ptr<RaceState> const& state,
                      StatusOr<ReadSourceResult> result,
                      std::unique_ptr<ObjectReadSource> source,
                      std::unique_ptr<char[]> buffer) {
  bool expected = false;
  if (state->resolved.compare_exchange_strong(expected, true)) {
    state->promise.set_value(RaceResult{std::move(result), std::move(source),
                                        std::move(buffer)});
  } else {
    source->Close();
  }
}

// Runs a single read attempt. If child is provided, it reads from that existing
// source (e.g. primary attempt on an established connection). Otherwise, it
// opens a new child at @p offset and @p generation via @p factory.
// Successful reads resolve the race immediately. Failing attempts only resolve
// the race if all other active attempts have also completed and failed.
void RunAttempt(std::shared_ptr<RaceState> const& state,
                HedgedObjectReadSource::ChildFactory const& factory,
                std::unique_ptr<ObjectReadSource> child,
                std::unique_ptr<char[]> buffer, std::int64_t offset,
                std::optional<std::int64_t> generation, std::size_t n,
                std::shared_ptr<HedgingThreadPool> release_slot) {
  // Releases the acquired hedge concurrency slot upon function exit across
  // all code paths. For primary attempts, release_slot is nullptr.
  struct SlotGuard {
    std::shared_ptr<HedgingThreadPool> pool;
    ~SlotGuard() {
      if (pool) pool->ReleaseHedgeSlot();
    }
  } guard{std::move(release_slot)};

  if (!child) {
    StatusOr<std::unique_ptr<ObjectReadSource>> source =
        factory(offset, generation);
    if (!source) {
      ResolveOnError(state, std::move(source).status());
      return;
    }
    child = *std::move(source);
  }

  if (!buffer) {
    buffer.reset(new (std::nothrow) char[n]);
    if (!buffer) {
      ResolveOnError(state, google::cloud::internal::ResourceExhaustedError(
                                "Out of memory allocating hedge buffer",
                                GCP_ERROR_INFO()));
      return;
    }
  }

  StatusOr<ReadSourceResult> result = child->Read(buffer.get(), n);
  if (!result.ok()) {
    child->Close();
    ResolveOnError(state, std::move(result).status());
    return;
  }

  ResolveOnSuccess(state, std::move(result), std::move(child),
                   std::move(buffer));
}

}  // namespace

HedgedObjectReadSource::HedgedObjectReadSource(
    std::shared_ptr<ThreadPool> read_pool,
    std::shared_ptr<HedgingThreadPool> hedge_pool, ChildFactory child_factory,
    std::chrono::milliseconds delay, int max_hedges, std::size_t max_buffer,
    std::int64_t current_offset, OffsetDirection offset_direction,
    std::optional<std::int64_t> generation)
    : read_pool_(std::move(read_pool)),
      hedge_pool_(std::move(hedge_pool)),
      child_factory_(std::move(child_factory)),
      delay_(delay),
      max_hedges_(max_hedges),
      max_buffer_(max_buffer),
      current_offset_(current_offset),
      offset_direction_(offset_direction),
      generation_(generation) {}

HedgedObjectReadSource::HedgedObjectReadSource(
    std::shared_ptr<ThreadPool> read_pool,
    std::shared_ptr<HedgingThreadPool> hedge_pool, ChildFactory child_factory,
    std::chrono::milliseconds delay, int max_hedges, std::size_t max_buffer)
    : HedgedObjectReadSource(std::move(read_pool), std::move(hedge_pool),
                             std::move(child_factory), delay, max_hedges,
                             max_buffer, /*current_offset=*/0,
                             /*offset_direction=*/kFromBeginning,
                             /*generation=*/std::nullopt) {}

HedgedObjectReadSource::HedgedObjectReadSource(
    std::shared_ptr<ThreadPool> read_pool,
    std::shared_ptr<HedgingThreadPool> hedge_pool,
    SimpleChildFactory child_factory, std::chrono::milliseconds delay,
    int max_hedges, std::size_t max_buffer)
    : HedgedObjectReadSource(
          std::move(read_pool), std::move(hedge_pool),
          [f = std::move(child_factory)](
              std::int64_t, std::optional<std::int64_t>) { return f(); },
          delay, max_hedges, max_buffer) {}

bool HedgedObjectReadSource::IsOpen() const {
  if (active_child_) return active_child_->IsOpen();
  return !is_closed_;
}

StatusOr<HttpResponse> HedgedObjectReadSource::Close() {
  is_closed_ = true;
  if (active_child_) return active_child_->Close();
  // The source was never read from, there is no child (or HTTP response) to
  // close.
  return HttpResponse{HttpStatusCode::kOk, {}, {}};
}

void HedgedObjectReadSource::UpdateState(
    StatusOr<ReadSourceResult> const& result) {
  if (!result) return;
  if (result->generation) generation_ = result->generation;
  if (result->transformation.value_or("") == "gunzipped") {
    is_gunzipped_ = true;
    offset_direction_ = kFromBeginning;
  }
  if (offset_direction_ == kFromEnd) {
    current_offset_ -= static_cast<std::int64_t>(result->bytes_received);
  } else {
    current_offset_ += static_cast<std::int64_t>(result->bytes_received);
  }
}

StatusOr<ReadSourceResult> HedgedObjectReadSource::Read(char* buf,
                                                        std::size_t n) {
  if (is_closed_) {
    return ReadSourceResult{0, HttpResponse{HttpStatusCode::kOk, {}, {}}};
  }

  // Decompressive transcoding does not respect byte ranges (HTTP 206).
  // Mid-stream hedging would have to re-read and discard from offset 0, which
  // is worse than reading directly on the active child.
  if (is_gunzipped_) {
    if (!active_child_) {
      StatusOr<std::unique_ptr<ObjectReadSource>> child =
          child_factory_(current_offset_, generation_);
      if (!child) return std::move(child).status();
      active_child_ = *std::move(child);
    }
    StatusOr<ReadSourceResult> result = active_child_->Read(buf, n);
    UpdateState(result);
    return result;
  }

  // Large reads avoid the extra memory overhead of staging buffers per racing
  // attempt and read directly into the caller's buffer.
  if (n > max_buffer_) {
    if (!active_child_) {
      StatusOr<std::unique_ptr<ObjectReadSource>> child =
          child_factory_(current_offset_, generation_);
      if (!child) return std::move(child).status();
      active_child_ = *std::move(child);
    }
    StatusOr<ReadSourceResult> result = active_child_->Read(buf, n);
    UpdateState(result);
    return result;
  }

  if (max_hedges_ <= 0 || !read_pool_ || !hedge_pool_) {
    if (!active_child_) {
      StatusOr<std::unique_ptr<ObjectReadSource>> child =
          child_factory_(current_offset_, generation_);
      if (!child) return std::move(child).status();
      active_child_ = *std::move(child);
    }
    StatusOr<ReadSourceResult> result = active_child_->Read(buf, n);
    UpdateState(result);
    return result;
  }

  auto state = std::make_shared<RaceState>();
  auto future = state->promise.get_future();
  state->active_attempts.store(1);

  std::unique_ptr<char[]> primary_buf;
  if (primary_buffer_ && primary_buffer_capacity_ >= n) {
    primary_buf = std::move(primary_buffer_);
  }

  auto child_holder =
      std::make_shared<std::unique_ptr<ObjectReadSource>>(
          std::move(active_child_));
  auto buf_holder =
      std::make_shared<std::unique_ptr<char[]>>(std::move(primary_buf));

  auto primary = [state, factory = child_factory_, child_holder, buf_holder,
                  offset = current_offset_, gen = generation_, n]() {
    RunAttempt(state, factory, std::move(*child_holder), std::move(*buf_holder),
               offset, gen, n, nullptr);
  };
  // The primary attempt is scheduled on the dedicated read pool.
  // If the pool is shutting down run the attempt inline, the read must
  // complete either way.
  if (!read_pool_->Enqueue(primary)) primary();

  for (int hedges_dispatched = 0; hedges_dispatched < max_hedges_;) {
    if (future.wait_for(delay_) != std::future_status::timeout) break;
    if (!hedge_pool_->TryAcquireHedgeToken()) {
      // When delay_ is 0ms (or token acquisition fails), back off briefly on
      // the future instead of busy-spinning if tokens or concurrency slots are
      // temporarily exhausted.
      if (delay_ == std::chrono::milliseconds::zero()) {
        if (future.wait_for(std::chrono::milliseconds(10)) !=
            std::future_status::timeout) {
          break;
        }
      }
      continue;
    }
    state->active_attempts.fetch_add(1);
    auto hedge = [state, factory = child_factory_, offset = current_offset_,
                  gen = generation_, n, pool = hedge_pool_]() {
      RunAttempt(state, factory, /*child=*/nullptr, /*buffer=*/nullptr, offset,
                 gen, n, pool);
    };
    if (!hedge_pool_->Enqueue(hedge)) {
      hedge_pool_->ReleaseHedgeSlot();
      if (state->active_attempts.fetch_sub(1) == 1) {
        bool expected = false;
        if (state->resolved.compare_exchange_strong(expected, true)) {
          Status error;
          {
            std::lock_guard<std::mutex> lock(state->mu);
            error = std::move(state->last_error);
          }
          state->promise.set_value(
              RaceResult{std::move(error), nullptr, nullptr});
        }
      }
      break;
    }
    ++hedges_dispatched;
  }

  RaceResult race = future.get();
  active_child_ = std::move(race.source);
  if (race.result.ok()) {
    UpdateState(race.result);
    if (race.result->bytes_received > 0) {
      std::memcpy(buf, race.buffer.get(), race.result->bytes_received);
    }
    primary_buffer_ = std::move(race.buffer);
    primary_buffer_capacity_ = n;
  }
  return race.result;
}

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
