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
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <chrono>
#include <algorithm>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {

/**
 * A lazy, dynamically-scaling thread pool with integrated Concurrency & QPS Rate Limiters.
 * Starts with 0 threads, and spawns worker threads on-demand up to max_threads.
 * Enforces dual throttle gates: max concurrent active hedges, and maximum spawned hedges per second.
 */
class HedgingThreadPool {
 public:
  explicit HedgingThreadPool(std::size_t max_threads, double rate_limit, double capacity, std::int64_t max_concurrent)
      : max_threads_(max_threads), idle_threads_(0), stop_(false),
        rate_limit_(rate_limit), tokens_capacity_(capacity), tokens_(capacity),
        last_refill_(std::chrono::steady_clock::now()),
        max_concurrent_hedges_(max_concurrent), active_concurrent_hedges_(0) {}

  void Enqueue(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (stop_) return;
      tasks_.push(std::move(task));

      // Lazy Spawning: Only spawn a new thread if we have no idle threads
      // and we haven't hit our maximum concurrency ceiling yet.
      if (idle_threads_ == 0 && workers_.size() < max_threads_) {
        SpawnWorker();
      }
    }
    cv_.notify_one();
  }

  bool TryAcquireHedgeToken() {
    // 1. Gate 1: Check Active Concurrency Limit (Instantaneous active thread ceiling)
    if (max_concurrent_hedges_ > 0) {
      if (active_concurrent_hedges_.load(std::memory_order_relaxed) >= max_concurrent_hedges_) {
        return false;
      }
    }

    // 2. Gate 2: Check QPS Limit (Token Bucket)
    if (rate_limit_ > 0.0) {
      std::lock_guard<std::mutex> lock(limiter_mutex_);
      Refill();
      if (tokens_ < 1.0) {
        return false;
      }
      tokens_ -= 1.0;
    }

    // Successfully acquired: Increment active concurrent counter
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

  ~HedgingThreadPool() {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      stop_ = true;
    }
    cv_.notify_all();
    for (std::thread& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }

 private:
  void SpawnWorker() {
    workers_.emplace_back([this] {
      while (true) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(this->queue_mutex_);
          this->idle_threads_++;
          this->cv_.wait(lock, [this] {
            return this->stop_ || !this->tasks_.empty();
          });
          this->idle_threads_--;

          if (this->stop_ && this->tasks_.empty()) {
            return;
          }
          task = std::move(this->tasks_.front());
          this->tasks_.pop();
        }
        task();
      }
    });
  }

  void Refill() {
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_refill_).count();
    last_refill_ = now;
    tokens_ = (std::min)(tokens_capacity_, tokens_ + duration * rate_limit_);
  }

  std::size_t max_threads_;
  std::size_t idle_threads_;
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex queue_mutex_;
  std::condition_variable cv_;
  bool stop_;

  // Token Bucket Rate Limiter
  double rate_limit_;
  double tokens_capacity_;
  double tokens_;
  std::chrono::steady_clock::time_point last_refill_;
  std::mutex limiter_mutex_;

  // Concurrency Limiter
  std::int64_t max_concurrent_hedges_;
  std::atomic<std::int64_t> active_concurrent_hedges_;
};

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGING_THREAD_POOL_H
