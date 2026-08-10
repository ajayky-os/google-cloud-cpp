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
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {
namespace {

TEST(HedgingThreadPoolTest, EnqueueAndExecute) {
  HedgingThreadPool pool(2, 0.0, 0.0, 0);
  std::atomic<int> counter{0};
  
  pool.Enqueue([&]() { counter++; });
  pool.Enqueue([&]() { counter++; });
  
  // Wait a bit for threads to execute
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  
  EXPECT_EQ(counter.load(), 2);
}

TEST(HedgingThreadPoolTest, MaxConcurrentHedgesLimit) {
  // Max 1 concurrent hedge allowed
  HedgingThreadPool pool(5, 0.0, 0.0, 1);
  
  EXPECT_TRUE(pool.TryAcquireHedgeToken());
  EXPECT_FALSE(pool.TryAcquireHedgeToken()); // Should fail because 1 is active
  
  pool.ReleaseHedgeSlot();
  
  EXPECT_TRUE(pool.TryAcquireHedgeToken()); // Should succeed now
}

TEST(HedgingThreadPoolTest, RateLimiter) {
  // Rate limit of 5.0 tokens per second (1 token per 200ms), capacity 2
  HedgingThreadPool pool(5, 5.0, 2.0, 0);
  
  EXPECT_TRUE(pool.TryAcquireHedgeToken());
  EXPECT_TRUE(pool.TryAcquireHedgeToken());
  EXPECT_FALSE(pool.TryAcquireHedgeToken()); // Capacity exhausted
  
  std::this_thread::sleep_for(std::chrono::milliseconds(250)); // Wait for > 200ms (1 token)
  
  EXPECT_TRUE(pool.TryAcquireHedgeToken());
  EXPECT_FALSE(pool.TryAcquireHedgeToken());
}

}  // namespace
}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
