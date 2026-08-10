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
#include <mutex>
#include <vector>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {

/**
 * Tracks a rolling p99 of observed read latencies, bucketed by read size.
 *
 * `HedgedObjectReadSource` records the latency of each successful stream open
 * (including its initial read) and asks for the hedge delay before starting a
 * new race. The delay adapts to the observed tail latency: a hedge only
 * starts once the primary is slower than most (p99) previous opens. Until a
 * bucket has enough samples a static, size-based fallback ladder is used.
 *
 * This class is thread-safe. Share one instance per connection so samples
 * accumulate across streams.
 */
class DynamicHedgeThresholdManager {
 public:
  DynamicHedgeThresholdManager() = default;

  /// Records the latency of a successful read of @p size bytes.
  void RecordLatency(std::size_t size, std::chrono::milliseconds latency);

  /**
   * Returns the hedge delay for a read of @p size bytes.
   *
   * The delay is @p multiplier times the rolling p99 of the size bucket, or a
   * static fallback until the bucket has `kMinSamplesForPercentile` samples.
   * The result is never less than @p min_delay.
   */
  std::chrono::milliseconds CalculateHedgeDelay(
      std::size_t size, double multiplier, std::chrono::milliseconds min_delay);

 private:
  struct SizeBucket {
    std::mutex mu;
    std::vector<std::chrono::milliseconds> samples;
    std::size_t index = 0;
  };

  SizeBucket& GetBucket(std::size_t size);
  static std::chrono::milliseconds GetFallbackDelay(std::size_t size);

  static constexpr std::size_t kNumBuckets = 15;
  static constexpr std::size_t kMaxSamples = 100;
  static constexpr std::size_t kMinSamplesForPercentile = 10;

  SizeBucket buckets_[kNumBuckets];
};

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_DYNAMIC_HEDGE_THRESHOLD_MANAGER_H
