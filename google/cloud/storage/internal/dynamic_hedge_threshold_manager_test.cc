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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {
namespace {

using ::testing::Eq;

auto constexpr kSmallRead = std::size_t{4 * 1024};

TEST(DynamicHedgeThresholdManagerTest, FallbackDelayWithoutSamples) {
  DynamicHedgeThresholdManager manager;
  // With no samples the manager uses a static, size-based fallback. For small
  // reads that fallback is 100ms, so a larger `min_delay` must win.
  EXPECT_THAT(manager.CalculateHedgeDelay(kSmallRead, /*multiplier=*/1.2,
                                          std::chrono::milliseconds(500)),
              Eq(std::chrono::milliseconds(500)));
  // ... and a smaller `min_delay` yields the fallback itself.
  EXPECT_THAT(manager.CalculateHedgeDelay(kSmallRead, /*multiplier=*/1.2,
                                          std::chrono::milliseconds(10)),
              Eq(std::chrono::milliseconds(100)));
}

TEST(DynamicHedgeThresholdManagerTest, UsesPercentileWithEnoughSamples) {
  DynamicHedgeThresholdManager manager;
  // Record 100 identical samples; the p99 is then exactly the sample value
  // and the delay is `multiplier` times that value.
  for (int i = 0; i != 100; ++i) {
    manager.RecordLatency(kSmallRead, std::chrono::milliseconds(1000));
  }
  EXPECT_THAT(manager.CalculateHedgeDelay(kSmallRead, /*multiplier=*/1.5,
                                          std::chrono::milliseconds(10)),
              Eq(std::chrono::milliseconds(1500)));
}

TEST(DynamicHedgeThresholdManagerTest, MinDelayIsALowerBound) {
  DynamicHedgeThresholdManager manager;
  for (int i = 0; i != 100; ++i) {
    manager.RecordLatency(kSmallRead, std::chrono::milliseconds(10));
  }
  // The computed delay (1.2 * 10ms = 12ms) is below `min_delay`.
  EXPECT_THAT(manager.CalculateHedgeDelay(kSmallRead, /*multiplier=*/1.2,
                                          std::chrono::milliseconds(500)),
              Eq(std::chrono::milliseconds(500)));
}

TEST(DynamicHedgeThresholdManagerTest, BucketsAreIndependent) {
  DynamicHedgeThresholdManager manager;
  for (int i = 0; i != 100; ++i) {
    manager.RecordLatency(kSmallRead, std::chrono::milliseconds(1000));
  }
  // A read size in a different bucket has no samples yet, and uses its own
  // (larger) fallback delay.
  auto constexpr kLargeRead = std::size_t{16 * 1024 * 1024};
  EXPECT_THAT(manager.CalculateHedgeDelay(kLargeRead, /*multiplier=*/1.2,
                                          std::chrono::milliseconds(10)),
              Eq(std::chrono::milliseconds(1500)));
}

TEST(DynamicHedgeThresholdManagerTest, RollingWindowUsesRecentSamples) {
  DynamicHedgeThresholdManager manager;
  // Fill the window with slow samples, then overwrite it with fast ones. The
  // computed delay must reflect the recent (fast) samples only.
  for (int i = 0; i != 100; ++i) {
    manager.RecordLatency(kSmallRead, std::chrono::milliseconds(5000));
  }
  for (int i = 0; i != 100; ++i) {
    manager.RecordLatency(kSmallRead, std::chrono::milliseconds(100));
  }
  EXPECT_THAT(manager.CalculateHedgeDelay(kSmallRead, /*multiplier=*/1.0,
                                          std::chrono::milliseconds(10)),
              Eq(std::chrono::milliseconds(100)));
}

}  // namespace
}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
