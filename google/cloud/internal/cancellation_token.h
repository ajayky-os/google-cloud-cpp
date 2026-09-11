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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_INTERNAL_CANCELLATION_TOKEN_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_INTERNAL_CANCELLATION_TOKEN_H

#include "google/cloud/version.h"
#include <atomic>
#include <functional>
#include <map>
#include <mutex>

namespace google {
namespace cloud {
namespace rest_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

/**
 * Aborts in-flight HTTP transfers from another thread.
 *
 * A transfer started with a token (see `CancellationTokenOption`) checks the
 * token whenever it is about to wait for the network. Once the token is
 * cancelled the transfer fails with `kCancelled` and its connection is
 * discarded. A transfer that has not started yet fails before sending
 * anything.
 *
 * While a transfer waits it registers a wakeup callback, so `Cancel()`
 * interrupts the wait immediately rather than at its next timeout. Any number
 * of transfers may share a token, and cancellation is permanent.
 *
 * All member functions are thread-safe.
 */
class CancellationToken {
 public:
  CancellationToken() = default;

  CancellationToken(CancellationToken const&) = delete;
  CancellationToken& operator=(CancellationToken const&) = delete;

  /// Cancels every current and future transfer using this token.
  void Cancel() {
    cancelled_.store(true);
    std::lock_guard<std::mutex> lock(mu_);
    for (auto const& kv : wakeups_) kv.second();
  }

  bool cancelled() const { return cancelled_.load(); }

  /**
   * Registers a callback that interrupts the wait of the transfer identified
   * by @p transfer. The callback runs on the thread calling `Cancel()`, under
   * the token's lock, and must stay valid until `RemoveWakeup()` is called
   * with the same @p transfer.
   */
  void AddWakeup(void const* transfer, std::function<void()> wakeup) {
    std::lock_guard<std::mutex> lock(mu_);
    wakeups_[transfer] = std::move(wakeup);
  }

  void RemoveWakeup(void const* transfer) {
    std::lock_guard<std::mutex> lock(mu_);
    wakeups_.erase(transfer);
  }

 private:
  std::atomic<bool> cancelled_{false};
  std::mutex mu_;
  std::map<void const*, std::function<void()>> wakeups_;  // GUARDED_BY(mu_)
};

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace rest_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_INTERNAL_CANCELLATION_TOKEN_H
