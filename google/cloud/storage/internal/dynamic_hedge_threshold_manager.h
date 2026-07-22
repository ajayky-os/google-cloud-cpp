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
#include <thread>
#include <atomic>
#include <memory>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {

/**
 * Tracks the rolling p95 latency for GCS Object reads categorized by size bucket.
 * Also owns a thread registry ("trash bin") to safely reap orphaned hedged requests.
 */
class DynamicHedgeThresholdManager {
 public:
  DynamicHedgeThresholdManager() = default;
  ~DynamicHedgeThresholdManager();

  // Records a successful read latency for a given byte size.
  void RecordLatency(std::size_t size, std::chrono::milliseconds latency);
  
  // Metrics tracking
  void RecordHedgeResult(bool hedge_won);
  std::pair<std::uint64_t, std::uint64_t> GetHedgeMetrics() const;

  // Calculates the hedge delay based on the rolling p95 of the size bucket.
  std::chrono::milliseconds CalculateHedgeDelay(
      std::size_t size, double multiplier,
      std::chrono::milliseconds min_delay);

  // Registers a stalled background thread so it can be reaped when it eventually finishes,
  // preventing user thread blocking and thread leaks.
  void RegisterOrphan(std::thread t, std::shared_ptr<std::atomic<bool>> is_done);

 private:
  struct SizeBucket {
    std::mutex mu;
    std::vector<std::chrono::milliseconds> samples;
    std::size_t index = 0;
    bool is_full = false;
  };

  SizeBucket& GetBucket(std::size_t size);
  std::chrono::milliseconds GetFallbackDelay(std::size_t size);

  static constexpr std::size_t kNumBuckets = 15;
  SizeBucket buckets_[kNumBuckets];
  static constexpr std::size_t kMaxSamples = 100;
  static constexpr std::size_t kMinSamplesForP95 = 10;

  // Orphan thread registry
  struct Orphan {
      std::thread t;
      std::shared_ptr<std::atomic<bool>> is_done;
  };
  std::mutex orphans_mu_;
  std::vector<Orphan> orphans_;
  
  std::atomic<std::uint64_t> primary_wins_{0};
  std::atomic<std::uint64_t> hedge_wins_{0};
};

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_DYNAMIC_HEDGE_THRESHOLD_MANAGER_H
