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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGED_OBJECT_READ_SOURCE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGED_OBJECT_READ_SOURCE_H

#include "google/cloud/storage/internal/dynamic_hedge_threshold_manager.h"
#include "google/cloud/storage/internal/object_read_source.h"
#include "google/cloud/storage/internal/object_requests.h"
#include <chrono>
#include <functional>
#include <memory>
#include <vector>

namespace google {
namespace cloud {
namespace storage {
namespace internal {

class HedgedObjectReadSource : public ObjectReadSource {
 public:
  using ChildFactory = std::function<std::unique_ptr<ObjectReadSource>()>;

  HedgedObjectReadSource(
      std::shared_ptr<DynamicHedgeThresholdManager> hedge_manager,
      ReadObjectRangeRequest request,
      ChildFactory child_factory,
      bool enable_hedging,
      double multiplier,
      std::chrono::milliseconds min_delay,
      int max_hedges);

  ~HedgedObjectReadSource() override = default;

  bool IsOpen() const override;
  StatusOr<HttpResponse> Close() override;
  StatusOr<ReadSourceResult> Read(char* buf, std::size_t n) override;

 private:
  std::shared_ptr<DynamicHedgeThresholdManager> hedge_manager_;
  ReadObjectRangeRequest request_;
  ChildFactory child_factory_;
  
  bool enable_hedging_;
  double multiplier_;
  std::chrono::milliseconds min_delay_;
  int max_hedges_;

  std::unique_ptr<ObjectReadSource> active_child_;
};

}  // namespace internal
}  // namespace storage
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGED_OBJECT_READ_SOURCE_H
