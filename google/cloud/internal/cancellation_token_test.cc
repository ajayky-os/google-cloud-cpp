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

#include "google/cloud/internal/cancellation_token.h"
#include <gmock/gmock.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace google {
namespace cloud {
namespace rest_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

using ::testing::Eq;

TEST(CancellationToken, InitialState) {
  CancellationToken token;
  EXPECT_FALSE(token.cancelled());
}

TEST(CancellationToken, CancelIsPermanent) {
  CancellationToken token;
  token.Cancel();
  EXPECT_TRUE(token.cancelled());
  token.Cancel();
  EXPECT_TRUE(token.cancelled());
}

TEST(CancellationToken, CancelRunsRegisteredWakeups) {
  CancellationToken token;
  int const transfer_a = 0;
  int const transfer_b = 0;
  int wakeups_a = 0;
  int wakeups_b = 0;
  token.AddWakeup(&transfer_a, [&wakeups_a] { ++wakeups_a; });
  token.AddWakeup(&transfer_b, [&wakeups_b] { ++wakeups_b; });
  token.RemoveWakeup(&transfer_b);

  token.Cancel();
  EXPECT_THAT(wakeups_a, Eq(1));
  EXPECT_THAT(wakeups_b, Eq(0));

  // Once cancelled, the flag is what a transfer observes; the wakeup only
  // interrupts a wait that is already in progress.
  token.RemoveWakeup(&transfer_a);
  token.Cancel();
  EXPECT_THAT(wakeups_a, Eq(1));
}

TEST(CancellationToken, AddWakeupReplacesPreviousForSameTransfer) {
  CancellationToken token;
  int const transfer = 0;
  int old_wakeups = 0;
  int new_wakeups = 0;
  token.AddWakeup(&transfer, [&old_wakeups] { ++old_wakeups; });
  token.AddWakeup(&transfer, [&new_wakeups] { ++new_wakeups; });
  token.Cancel();
  EXPECT_THAT(old_wakeups, Eq(0));
  EXPECT_THAT(new_wakeups, Eq(1));
}

TEST(CancellationToken, CancelFromAnotherThreadInterruptsWait) {
  // Models a transfer thread blocked in a wait, and a wakeup that interrupts
  // it, the way `CurlImpl` uses `curl_multi_wakeup()`.
  CancellationToken token;
  std::mutex mu;
  std::condition_variable cv;
  bool woken = false;
  int const transfer = 0;
  token.AddWakeup(&transfer, [&] {
    std::lock_guard<std::mutex> lock(mu);
    woken = true;
    cv.notify_all();
  });

  std::thread canceller([&token] { token.Cancel(); });
  {
    std::unique_lock<std::mutex> lock(mu);
    cv.wait(lock, [&woken] { return woken; });
  }
  canceller.join();
  EXPECT_TRUE(token.cancelled());
  token.RemoveWakeup(&transfer);
}

TEST(CancellationToken, WaitForReturnsOnTimeoutWhenNotCancelled) {
  CancellationToken token;
  EXPECT_FALSE(token.WaitFor(std::chrono::milliseconds(1)));
}

TEST(CancellationToken, WaitForReturnsImmediatelyWhenAlreadyCancelled) {
  CancellationToken token;
  token.Cancel();
  // A long delay: the test would time out if the wait were not short-circuited.
  EXPECT_TRUE(token.WaitFor(std::chrono::hours(1)));
}

TEST(CancellationToken, WaitForWakesOnCancel) {
  CancellationToken token;
  std::mutex mu;
  std::condition_variable waiting_cv;
  bool waiting = false;
  bool cancelled = false;

  std::thread waiter([&] {
    {
      std::lock_guard<std::mutex> lock(mu);
      waiting = true;
      waiting_cv.notify_all();
    }
    cancelled = token.WaitFor(std::chrono::hours(1));
  });
  {
    std::unique_lock<std::mutex> lock(mu);
    waiting_cv.wait(lock, [&waiting] { return waiting; });
  }
  token.Cancel();
  waiter.join();
  EXPECT_TRUE(cancelled);
}

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace rest_internal
}  // namespace cloud
}  // namespace google
