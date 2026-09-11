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
#include <condition_variable>
#include <mutex>
#include <thread>

namespace google {
namespace cloud {
namespace rest_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

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
  EXPECT_EQ(wakeups_a, 1);
  EXPECT_EQ(wakeups_b, 0);

  // Once cancelled, the flag is what a transfer observes; the wakeup only
  // interrupts a wait that is already in progress.
  token.RemoveWakeup(&transfer_a);
  token.Cancel();
  EXPECT_EQ(wakeups_a, 1);
}

TEST(CancellationToken, AddWakeupReplacesPreviousForSameTransfer) {
  CancellationToken token;
  int const transfer = 0;
  int old_wakeups = 0;
  int new_wakeups = 0;
  token.AddWakeup(&transfer, [&old_wakeups] { ++old_wakeups; });
  token.AddWakeup(&transfer, [&new_wakeups] { ++new_wakeups; });
  token.Cancel();
  EXPECT_EQ(old_wakeups, 0);
  EXPECT_EQ(new_wakeups, 1);
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

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace rest_internal
}  // namespace cloud
}  // namespace google
