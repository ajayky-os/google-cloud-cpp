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
#include <thread>
#include <future>
#include <cstring>
#include <atomic>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {

HedgedObjectReadSource::HedgedObjectReadSource(
    std::shared_ptr<DynamicHedgeThresholdManager> hedge_manager,
    ReadObjectRangeRequest request,
    ChildFactory child_factory,
    bool enable_hedging,
    double multiplier,
    std::chrono::milliseconds min_delay,
    int max_hedges)
    : hedge_manager_(std::move(hedge_manager)),
      request_(std::move(request)),
      child_factory_(std::move(child_factory)),
      enable_hedging_(enable_hedging),
      multiplier_(multiplier),
      min_delay_(min_delay),
      max_hedges_(max_hedges) {}

bool HedgedObjectReadSource::IsOpen() const {
  if (active_child_) return active_child_->IsOpen();
  return true;
}

StatusOr<HttpResponse> HedgedObjectReadSource::Close() {
  if (active_child_) return active_child_->Close();
  return HttpResponse{200, "", {}};
}

enum class SourceType { kPrimary, kHedge };

struct RaceResult {
    StatusOr<ReadSourceResult> result;
    std::unique_ptr<ObjectReadSource> winner_source;
    std::vector<char> winner_buffer;
    std::chrono::milliseconds duration;
    SourceType winner_type;
};

// A custom deleter for the shared_ptr to prevent it from deleting the
// ObjectReadSource since we extract it back into a unique_ptr.
struct NoOpDeleter {
  void operator()(ObjectReadSource*) const {}
};

StatusOr<ReadSourceResult> HedgedObjectReadSource::Read(char* buf, std::size_t n) {
  if (!active_child_) {
      active_child_ = child_factory_();
      if (!active_child_) {
          return google::cloud::internal::UnknownError("Failed to initialize active stream", GCP_ERROR_INFO());
      }
  }

  if (!enable_hedging_ || max_hedges_ <= 0) {
      return active_child_->Read(buf, n);
  }

  auto target_delay = hedge_manager_->CalculateHedgeDelay(n, multiplier_, min_delay_);

  auto promise = std::make_shared<std::promise<RaceResult>>();
  auto future = promise->get_future();
  auto resolved = std::make_shared<std::atomic<bool>>(false);
  
  auto start_time = std::chrono::steady_clock::now();

  auto shared_primary = std::shared_ptr<ObjectReadSource>(active_child_.release(), NoOpDeleter{});
  auto primary_done = std::make_shared<std::atomic<bool>>(false);

  // Spawn primary thread
  std::thread primary_thread([shared_primary, n, start_time, promise, resolved, primary_done]() {
      std::vector<char> local_buf(n);
      auto res = shared_primary->Read(local_buf.data(), n);
      auto end_time = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
      
      bool expected = false;
      if (resolved->compare_exchange_strong(expected, true)) {
          std::unique_ptr<ObjectReadSource> winner_ptr(shared_primary.get());
          promise->set_value(RaceResult{std::move(res), std::move(winner_ptr), std::move(local_buf), duration, SourceType::kPrimary});
      }
      *primary_done = true;
  });

  std::thread hedge_thread;
  auto hedge_done = std::make_shared<std::atomic<bool>>(false);
  bool hedge_spawned = false;

  // Wait for the primary to finish or timeout
  if (future.wait_for(target_delay) == std::future_status::timeout) {
      auto shared_hedge = std::shared_ptr<ObjectReadSource>(child_factory_().release(), NoOpDeleter{});
      
      if (shared_hedge.get()) {
          hedge_spawned = true;
          // Spawn hedge thread
          hedge_thread = std::thread([shared_hedge, n, start_time, promise, resolved, hedge_done]() {
              std::vector<char> local_buf(n);
              auto res = shared_hedge->Read(local_buf.data(), n);
              auto end_time = std::chrono::steady_clock::now();
              auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
              
              bool expected = false;
              if (resolved->compare_exchange_strong(expected, true)) {
                  std::unique_ptr<ObjectReadSource> winner_ptr(shared_hedge.get());
                  promise->set_value(RaceResult{std::move(res), std::move(winner_ptr), std::move(local_buf), duration, SourceType::kHedge});
              }
              *hedge_done = true;
          });
      }
  }

  // Block the main user thread only until a winner emerges.
  auto final_result = future.get();

  if (final_result.winner_type == SourceType::kPrimary) {
      // Primary won. Safe to join immediately.
      primary_thread.join();
      // Orphan the hedge if it was spawned but lost.
      if (hedge_spawned && hedge_thread.joinable()) {
          hedge_manager_->RegisterOrphan(std::move(hedge_thread), hedge_done);
      }
  } else {
      // Hedge won. Safe to join immediately.
      hedge_thread.join();
      // Orphan the stalled primary so we don't block the user.
      if (primary_thread.joinable()) {
          hedge_manager_->RegisterOrphan(std::move(primary_thread), primary_done);
      }
  }
  
  if (final_result.result.ok()) {
      hedge_manager_->RecordLatency(n, final_result.duration);
  }

  active_child_ = std::move(final_result.winner_source);

  if (final_result.result.ok() && final_result.result->bytes_received > 0) {
      std::memcpy(buf, final_result.winner_buffer.data(), final_result.result->bytes_received);
  }

  return final_result.result;
}

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
