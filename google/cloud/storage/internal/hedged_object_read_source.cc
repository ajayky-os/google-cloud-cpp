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

  struct RaceResult {
      StatusOr<ReadSourceResult> result;
      std::unique_ptr<ObjectReadSource> winner_source;
      std::vector<char> winner_buffer;
      std::chrono::milliseconds duration;
  };

  std::promise<RaceResult> promise;
  auto future = promise.get_future();
  std::shared_ptr<std::atomic<bool>> resolved = std::make_shared<std::atomic<bool>>(false);
  
  auto start_time = std::chrono::steady_clock::now();

  // Spawn primary thread
  std::thread primary_thread([primary_source = std::move(active_child_), n, start_time, &promise, resolved]() mutable {
      std::vector<char> local_buf(n);
      auto res = primary_source->Read(local_buf.data(), n);
      auto end_time = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
      
      bool expected = false;
      if (resolved->compare_exchange_strong(expected, true)) {
          promise.set_value(RaceResult{std::move(res), std::move(primary_source), std::move(local_buf), duration});
      } else {
          primary_source->Close();
      }
  });

  std::thread hedge_thread;
  bool hedge_spawned = false;

  // Wait for the primary to finish or timeout
  if (future.wait_for(target_delay) == std::future_status::timeout) {
      hedge_spawned = true;
      
      hedge_thread = std::thread([this, n, start_time, &promise, resolved]() mutable {
          auto hedge_source = child_factory_();
          std::vector<char> local_buf(n);
          if (!hedge_source) {
             return; 
          }
          auto res = hedge_source->Read(local_buf.data(), n);
          auto end_time = std::chrono::steady_clock::now();
          auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
          
          bool expected = false;
          if (resolved->compare_exchange_strong(expected, true)) {
              promise.set_value(RaceResult{std::move(res), std::move(hedge_source), std::move(local_buf), duration});
          } else {
              hedge_source->Close();
          }
      });
  }

  auto final_result = future.get();
  
  primary_thread.join();
  if (hedge_spawned) {
      hedge_thread.join();
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
}  // namespace storage
}  // namespace cloud
}  // namespace google
