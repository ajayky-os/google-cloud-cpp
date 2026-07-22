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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_DYNAMIC_HEDGE_THRESHOLD_MANAGER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_DYNAMIC_HEDGE_THRESHOLD_MANAGER_H

#include "google/cloud/storage/version.h"
#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {

/**
 * Tracks the rolling p95 latency for GCS Object reads categorized by size bucket.
 */
class DynamicHedgeThresholdManager {
 public:
  DynamicHedgeThresholdManager() = default;
  ~DynamicHedgeThresholdManager() = default;

  // Records a successful read latency for a given byte size.
  void RecordLatency(std::size_t size, std::chrono::milliseconds latency);

  // Calculates the hedge delay based on the rolling p95 of the size bucket.
  // Returns fallback delays if insufficient samples are present.
  std::chrono::milliseconds CalculateHedgeDelay(
      std::size_t size, double multiplier,
      std::chrono::milliseconds min_delay);

 private:
  struct SizeBucket {
    std::mutex mu;
    std::vector<std::chrono::milliseconds> samples;
    std::size_t index = 0;
    bool is_full = false;
  };

  SizeBucket& GetBucket(std::size_t size);
  std::chrono::milliseconds GetFallbackDelay(std::size_t size);

  // We define fixed log2 buckets.
  // 0-8KB, 8-64KB, 64-128KB, 128-256KB, 256-512KB, 512-1MB,
  // 1-2MB, 2-8MB, 8-16MB, 16-32MB, 32-64MB, 64-128MB, 128-256MB, 256-512MB, 512MB+
  static constexpr std::size_t kNumBuckets = 15;
  SizeBucket buckets_[kNumBuckets];
  static constexpr std::size_t kMaxSamples = 100;
  static constexpr std::size_t kMinSamplesForP95 = 10;
};

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_DYNAMIC_HEDGE_THRESHOLD_MANAGER_H
