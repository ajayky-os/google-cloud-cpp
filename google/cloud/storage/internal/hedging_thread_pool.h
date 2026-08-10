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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGING_THREAD_POOL_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGING_THREAD_POOL_H

#include "google/cloud/storage/version.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {

/**
 * A lazy, dynamically-scaling thread pool with integrated hedge throttling.
 *
 * The pool starts with no threads and spawns workers on demand, up to
 * @p max_threads. Hedged requests are gated by `TryAcquireHedgeToken()`,
 * which enforces two limits: a maximum number of concurrently active hedges,
 * and a maximum rate of new hedges per second (a token bucket).
 */
class HedgingThreadPool {
 public:
  HedgingThreadPool(std::size_t max_threads, double rate_limit, double capacity,
                    std::int64_t max_concurrent)
      : max_threads_(max_threads),
        rate_limit_(rate_limit),
        tokens_capacity_(capacity),
        tokens_(capacity),
        last_refill_(std::chrono::steady_clock::now()),
        max_concurrent_hedges_(max_concurrent) {}

  ~HedgingThreadPool() {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      stop_ = true;
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable()) worker.join();
    }
  }

  /**
   * Schedule @p task to run on a pool thread.
   *
   * Returns false if the pool is shutting down, in which case the task is
   * *not* scheduled. Callers waiting on the task's side effects must handle
   * this case (e.g. by running the task inline), or they would block forever.
   */
  bool Enqueue(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (stop_) return false;
      tasks_.push(std::move(task));
      // Only spawn a new thread if there are no idle threads and the pool has
      // not reached its thread ceiling.
      if (idle_threads_ == 0 && workers_.size() < max_threads_) SpawnWorker();
    }
    cv_.notify_one();
    return true;
  }

  /**
   * Try to reserve capacity for one hedged request.
   *
   * On success the caller *must* eventually call `ReleaseHedgeSlot()`.
   */
  bool TryAcquireHedgeToken() {
    // Gate 1: the ceiling on concurrently active hedges.
    if (max_concurrent_hedges_ > 0 &&
        active_concurrent_hedges_.load(std::memory_order_relaxed) >=
            max_concurrent_hedges_) {
      return false;
    }

    // Gate 2: the rate limit on new hedges (token bucket).
    if (rate_limit_ > 0.0) {
      std::lock_guard<std::mutex> lock(limiter_mutex_);
      Refill();
      if (tokens_ < 1.0) return false;
      tokens_ -= 1.0;
    }

    if (max_concurrent_hedges_ > 0) {
      active_concurrent_hedges_.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
  }

  void ReleaseHedgeSlot() {
    if (max_concurrent_hedges_ > 0) {
      active_concurrent_hedges_.fetch_sub(1, std::memory_order_relaxed);
    }
  }

 private:
  void SpawnWorker() {
    workers_.emplace_back([this] {
      while (true) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(queue_mutex_);
          ++idle_threads_;
          cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
          --idle_threads_;
          if (stop_ && tasks_.empty()) return;
          task = std::move(tasks_.front());
          tasks_.pop();
        }
        task();
      }
    });
  }

  void Refill() {
    auto now = std::chrono::steady_clock::now();
    auto const elapsed =
        std::chrono::duration_cast<std::chrono::duration<double>>(now -
                                                                  last_refill_)
            .count();
    last_refill_ = now;
    tokens_ = (std::min)(tokens_capacity_, tokens_ + elapsed * rate_limit_);
  }

  std::size_t max_threads_;
  std::size_t idle_threads_ = 0;
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex queue_mutex_;
  std::condition_variable cv_;
  bool stop_ = false;

  // Token bucket rate limiter.
  double rate_limit_;
  double tokens_capacity_;
  double tokens_;
  std::chrono::steady_clock::time_point last_refill_;
  std::mutex limiter_mutex_;

  // Concurrency limiter.
  std::int64_t max_concurrent_hedges_;
  std::atomic<std::int64_t> active_concurrent_hedges_{0};
};

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGING_THREAD_POOL_H
