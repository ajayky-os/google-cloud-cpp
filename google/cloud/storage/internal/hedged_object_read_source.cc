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
#include <atomic>
#include <cstring>
#include <future>
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
  std::vector<char> buffer;
};

struct RaceState {
  std::promise<RaceResult> promise;
  std::atomic<bool> resolved{false};
};

// Opens a new child and performs its initial read, resolving the race if this
// attempt finishes first. Losing attempts close their child. Only the primary
// attempt resolves the race on an open error: a hedge that fails to open must
// not mask a slower, but successful, primary.
void RunAttempt(std::shared_ptr<RaceState> const& state,
                HedgedObjectReadSource::ChildFactory const& factory,
                std::size_t n, bool resolve_on_open_error,
                std::shared_ptr<HedgingThreadPool> release_slot) {
  struct SlotGuard {
    std::shared_ptr<HedgingThreadPool> pool;
    ~SlotGuard() {
      if (pool) pool->ReleaseHedgeSlot();
    }
  } guard{std::move(release_slot)};

  auto source = factory();
  if (!source) {
    if (!resolve_on_open_error) return;
    auto expected = false;
    if (state->resolved.compare_exchange_strong(expected, true)) {
      state->promise.set_value(
          RaceResult{std::move(source).status(), nullptr, {}});
    }
    return;
  }
  std::vector<char> buffer(n);
  auto result = (*source)->Read(buffer.data(), n);
  auto expected = false;
  if (state->resolved.compare_exchange_strong(expected, true)) {
    state->promise.set_value(
        RaceResult{std::move(result), *std::move(source), std::move(buffer)});
  } else {
    (*source)->Close();
  }
}

}  // namespace

HedgedObjectReadSource::HedgedObjectReadSource(
    std::shared_ptr<HedgingThreadPool> hedge_pool, ChildFactory child_factory,
    std::chrono::milliseconds delay, int max_hedges)
    : hedge_pool_(std::move(hedge_pool)),
      child_factory_(std::move(child_factory)),
      delay_(delay),
      max_hedges_(max_hedges) {}

bool HedgedObjectReadSource::IsOpen() const {
  if (active_child_) return active_child_->IsOpen();
  return true;
}

StatusOr<HttpResponse> HedgedObjectReadSource::Close() {
  if (active_child_) return active_child_->Close();
  // The source was never read from, there is no child (or HTTP response) to
  // close.
  return HttpResponse{HttpStatusCode::kOk, {}, {}};
}

StatusOr<ReadSourceResult> HedgedObjectReadSource::Read(char* buf,
                                                        std::size_t n) {
  // Only the stream open is hedged. Once a child has won the race all
  // subsequent reads continue on it, at its current offset, without any
  // thread hops or extra copies.
  if (active_child_) return active_child_->Read(buf, n);

  auto state = std::make_shared<RaceState>();
  auto future = state->promise.get_future();

  auto primary = [state, factory = child_factory_, n] {
    RunAttempt(state, factory, n, /*resolve_on_open_error=*/true, nullptr);
  };
  // If the pool is shutting down run the attempt inline, the read must
  // complete either way.
  if (!hedge_pool_->Enqueue(primary)) primary();

  for (int i = 0; i != max_hedges_; ++i) {
    if (future.wait_for(delay_) != std::future_status::timeout) break;
    if (!hedge_pool_->TryAcquireHedgeToken()) continue;
    auto hedge = [state, factory = child_factory_, n, pool = hedge_pool_] {
      RunAttempt(state, factory, n, /*resolve_on_open_error=*/false, pool);
    };
    if (!hedge_pool_->Enqueue(hedge)) {
      hedge_pool_->ReleaseHedgeSlot();
      break;
    }
  }

  auto race = future.get();
  active_child_ = std::move(race.source);
  if (race.result.ok() && race.result->bytes_received > 0) {
    std::memcpy(buf, race.buffer.data(), race.result->bytes_received);
  }
  return race.result;
}

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
