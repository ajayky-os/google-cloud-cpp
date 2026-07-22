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

#include "google/cloud/storage/internal/dynamic_hedge_threshold_manager.h"
#include <algorithm>
#include <cmath>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {

namespace {
constexpr std::size_t kKiB = 1024;
constexpr std::size_t kMiB = 1024 * 1024;
}  // namespace

DynamicHedgeThresholdManager::~DynamicHedgeThresholdManager() {
  std::lock_guard<std::mutex> lock(orphans_mu_);
  for (auto& orphan : orphans_) {
    if (orphan.t.joinable()) {
      orphan.t.join();
    }
  }
}

void DynamicHedgeThresholdManager::RegisterOrphan(std::thread t, std::shared_ptr<std::atomic<bool>> is_done) {
  std::lock_guard<std::mutex> lock(orphans_mu_);
  
  // Garbage collect completed orphans to prevent unbounded growth
  for (auto it = orphans_.begin(); it != orphans_.end(); ) {
      if (it->is_done->load()) {
          if (it->t.joinable()) {
              it->t.join();
          }
          it = orphans_.erase(it);
      } else {
          ++it;
      }
  }
  
  orphans_.push_back({std::move(t), std::move(is_done)});
}

DynamicHedgeThresholdManager::SizeBucket& DynamicHedgeThresholdManager::GetBucket(std::size_t size) {
  if (size < 8 * kKiB) return buckets_[0];
  if (size < 64 * kKiB) return buckets_[1];
  if (size < 128 * kKiB) return buckets_[2];
  if (size < 256 * kKiB) return buckets_[3];
  if (size < 512 * kKiB) return buckets_[4];
  if (size < 1 * kMiB) return buckets_[5];
  if (size < 2 * kMiB) return buckets_[6];
  if (size < 8 * kMiB) return buckets_[7];
  if (size < 16 * kMiB) return buckets_[8];
  if (size < 32 * kMiB) return buckets_[9];
  if (size < 64 * kMiB) return buckets_[10];
  if (size < 128 * kMiB) return buckets_[11];
  if (size < 256 * kMiB) return buckets_[12];
  if (size < 512 * kMiB) return buckets_[13];
  return buckets_[14];
}

std::chrono::milliseconds DynamicHedgeThresholdManager::GetFallbackDelay(std::size_t size) {
  if (size < 1 * kMiB) return std::chrono::milliseconds(100);
  if (size < 8 * kMiB) return std::chrono::milliseconds(500);
  if (size < 64 * kMiB) return std::chrono::milliseconds(1500);
  if (size < 256 * kMiB) return std::chrono::milliseconds(3000);
  return std::chrono::milliseconds(5000);
}

void DynamicHedgeThresholdManager::RecordLatency(std::size_t size, std::chrono::milliseconds latency) {
  auto& bucket = GetBucket(size);
  std::lock_guard<std::mutex> lk(bucket.mu);
  if (bucket.samples.size() < kMaxSamples) {
    bucket.samples.push_back(latency);
  } else {
    bucket.samples[bucket.index] = latency;
    bucket.is_full = true;
  }
  bucket.index = (bucket.index + 1) % kMaxSamples;
}

std::chrono::milliseconds DynamicHedgeThresholdManager::CalculateHedgeDelay(
    std::size_t size, double multiplier, std::chrono::milliseconds min_delay) {
  auto& bucket = GetBucket(size);
  std::vector<std::chrono::milliseconds> local_samples;
  {
    std::lock_guard<std::mutex> lk(bucket.mu);
    local_samples = bucket.samples;
  }

  if (local_samples.size() < kMinSamplesForP95) {
    auto fallback = GetFallbackDelay(size);
    return std::max(fallback, min_delay);
  }

  std::sort(local_samples.begin(), local_samples.end());
  
  // Calculate p95
  double p = 0.95;
  double index = p * (local_samples.size() - 1);
  std::size_t lower = static_cast<std::size_t>(std::floor(index));
  std::size_t upper = static_cast<std::size_t>(std::ceil(index));
  double weight = index - lower;

  auto p95_latency = local_samples[lower] + 
      std::chrono::duration_cast<std::chrono::milliseconds>(
          (local_samples[upper] - local_samples[lower]) * weight);

  auto target_delay = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::duration<double, std::milli>(p95_latency.count() * multiplier));

  return std::max(target_delay, min_delay);
}

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
