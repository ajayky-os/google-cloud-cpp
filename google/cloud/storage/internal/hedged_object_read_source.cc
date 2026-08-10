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
    std::shared_ptr<HedgingThreadPool> hedge_pool,
    ReadObjectRangeRequest request,
    ChildFactory child_factory,
    std::chrono::milliseconds min_delay,
    int max_hedges)
    : hedge_pool_(std::move(hedge_pool)),
      request_(std::move(request)),
      child_factory_(std::move(child_factory)),
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

StatusOr<ReadSourceResult> HedgedObjectReadSource::Read(char* buf, std::size_t n) {
  // Under the simplified model, we use the fixed delay safety floor (default: e.g. 500ms)
  auto target_delay = min_delay_;

  auto promise = std::make_shared<std::promise<RaceResult>>();
  auto future = promise->get_future();
  auto resolved = std::make_shared<std::atomic<bool>>(false);
  
  auto start_time = std::chrono::steady_clock::now();

  auto execute_task = [n, start_time, promise, resolved](
      std::shared_ptr<std::unique_ptr<ObjectReadSource>> shared_source,
      SourceType type,
      std::shared_ptr<HedgingThreadPool> pool_for_release = nullptr) {
      
      struct ConcurrencyGuard {
          std::shared_ptr<HedgingThreadPool> p;
          ~ConcurrencyGuard() { if (p) p->ReleaseHedgeSlot(); }
      } guard{pool_for_release};
      
      if (!*shared_source) return;
      
      std::vector<char> local_buf(n);
      auto res = (*shared_source)->Read(local_buf.data(), n);
      auto end_time = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
      
      bool expected = false;
      if (resolved->compare_exchange_strong(expected, true)) {
          promise->set_value(RaceResult{
              std::move(res), 
              std::move(*shared_source), 
              std::move(local_buf), 
              duration, 
              type
          });
      } else {
          if (*shared_source) (*shared_source)->Close();
      }
  };

  if (active_child_) {
      auto shared_primary = std::make_shared<std::unique_ptr<ObjectReadSource>>(std::move(active_child_));
      hedge_pool_->Enqueue([execute_task, shared_primary]() {
          execute_task(shared_primary, SourceType::kPrimary);
      });
  } else {
      hedge_pool_->Enqueue([execute_task, factory = child_factory_, promise, resolved]() {
          auto local_primary_status = factory();
          if (!local_primary_status) {
              bool expected = false;
              if (resolved->compare_exchange_strong(expected, true)) {
                  promise->set_value(RaceResult{local_primary_status.status(), nullptr, {}, std::chrono::milliseconds(0), SourceType::kPrimary});
              }
              return;
          }
          auto shared_primary = std::make_shared<std::unique_ptr<ObjectReadSource>>(*std::move(local_primary_status));
          execute_task(shared_primary, SourceType::kPrimary);
      });
  }

  if (future.wait_for(target_delay) == std::future_status::timeout) {
      if (hedge_pool_->TryAcquireHedgeToken()) {
          hedge_pool_->Enqueue([execute_task, factory = child_factory_, pool = hedge_pool_]() {
              auto hedge_source_status = factory();
              auto shared_hedge = std::make_shared<std::unique_ptr<ObjectReadSource>>();
              if (hedge_source_status) {
                  *shared_hedge = *std::move(hedge_source_status);
              }
              execute_task(shared_hedge, SourceType::kHedge, pool);
          });
      }
  }

  auto final_result = future.get();

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
