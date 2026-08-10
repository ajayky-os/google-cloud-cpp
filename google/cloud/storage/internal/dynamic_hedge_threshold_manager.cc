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

DynamicHedgeThresholdManager::SizeBucket&
DynamicHedgeThresholdManager::GetBucket(std::size_t size) {
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

std::chrono::milliseconds DynamicHedgeThresholdManager::GetFallbackDelay(
    std::size_t size) {
  if (size < 1 * kMiB) return std::chrono::milliseconds(100);
  if (size < 8 * kMiB) return std::chrono::milliseconds(500);
  if (size < 64 * kMiB) return std::chrono::milliseconds(1500);
  if (size < 256 * kMiB) return std::chrono::milliseconds(3000);
  return std::chrono::milliseconds(5000);
}

void DynamicHedgeThresholdManager::RecordLatency(
    std::size_t size, std::chrono::milliseconds latency) {
  auto& bucket = GetBucket(size);
  std::lock_guard<std::mutex> lk(bucket.mu);
  if (bucket.samples.size() < kMaxSamples) {
    bucket.samples.push_back(latency);
  } else {
    bucket.samples[bucket.index] = latency;
  }
  bucket.index = (bucket.index + 1) % kMaxSamples;
}

std::chrono::milliseconds DynamicHedgeThresholdManager::CalculateHedgeDelay(
    std::size_t size, double multiplier, std::chrono::milliseconds min_delay) {
  auto& bucket = GetBucket(size);
  std::vector<std::chrono::milliseconds> samples;
  {
    std::lock_guard<std::mutex> lk(bucket.mu);
    samples = bucket.samples;
  }

  if (samples.size() < kMinSamplesForPercentile) {
    return (std::max)(GetFallbackDelay(size), min_delay);
  }

  // Compute the p99 sample using linear interpolation.
  std::sort(samples.begin(), samples.end());
  auto const percentile = 0.99;
  auto const index = percentile * static_cast<double>(samples.size() - 1);
  auto const lower = static_cast<std::size_t>(std::floor(index));
  auto const upper = static_cast<std::size_t>(std::ceil(index));
  auto const weight = index - static_cast<double>(lower);
  auto const p99 =
      samples[lower] + std::chrono::duration_cast<std::chrono::milliseconds>(
                           (samples[upper] - samples[lower]) * weight);

  auto const target = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::duration<double, std::milli>(
          static_cast<double>(p99.count()) * multiplier));
  return (std::max)(target, min_delay);
}

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
