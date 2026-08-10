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

#include "google/cloud/storage/internal/hedging_thread_pool.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <thread>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {
namespace {

TEST(HedgingThreadPoolTest, EnqueueAndExecute) {
  HedgingThreadPool pool(2, 0.0, 0.0, 0);
  std::promise<void> p1;
  std::promise<void> p2;

  EXPECT_TRUE(pool.Enqueue([&p1] { p1.set_value(); }));
  EXPECT_TRUE(pool.Enqueue([&p2] { p2.set_value(); }));

  p1.get_future().get();
  p2.get_future().get();
}

TEST(HedgingThreadPoolTest, MaxConcurrentHedgesLimit) {
  // Only one concurrent hedge allowed.
  HedgingThreadPool pool(5, 0.0, 0.0, 1);

  EXPECT_TRUE(pool.TryAcquireHedgeToken());
  // Fails because one hedge is active.
  EXPECT_FALSE(pool.TryAcquireHedgeToken());

  pool.ReleaseHedgeSlot();

  EXPECT_TRUE(pool.TryAcquireHedgeToken());
}

TEST(HedgingThreadPoolTest, RateLimiter) {
  // A rate limit of 5.0 tokens per second (one token per 200ms), and a burst
  // capacity of 2 tokens.
  HedgingThreadPool pool(5, 5.0, 2.0, 0);

  EXPECT_TRUE(pool.TryAcquireHedgeToken());
  EXPECT_TRUE(pool.TryAcquireHedgeToken());
  // The burst capacity is exhausted.
  EXPECT_FALSE(pool.TryAcquireHedgeToken());

  // The refill is time-based, there is no way to inject a fake clock. Wait
  // longer than one token's refill period, with margin for slow machines.
  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  EXPECT_TRUE(pool.TryAcquireHedgeToken());
}

}  // namespace
}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
