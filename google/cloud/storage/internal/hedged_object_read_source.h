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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGED_OBJECT_READ_SOURCE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGED_OBJECT_READ_SOURCE_H

#include "google/cloud/storage/internal/hedging_thread_pool.h"
#include "google/cloud/storage/internal/object_read_source.h"
#include "google/cloud/storage/internal/retry_object_read_source.h"
#include "google/cloud/storage/version.h"
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {

/**
 * Hedge reads of an `ObjectReadSource` to reduce tail latency.
 *
 * Each `Read()` races the active child (or a newly opened child for the first
 * read) against one or more children created by @p child_factory at the
 * stream's current byte offset: a primary attempt starts immediately, and up to
 * @p max_hedges additional attempts start, staggered by @p delay, while no
 * attempt has completed. The first attempt to complete its read wins; losing
 * attempts are closed when they eventually complete. If a hedge wins, it
 * replaces the active child for subsequent reads.
 *
 * If decompressive transcoding is detected (`is_gunzipped_`), mid-stream range
 * hedging is bypassed and reads continue on the active child directly.
 *
 * Each racing attempt reads into its own buffer, because a losing attempt
 * keeps writing until it completes and must not touch the caller's buffer.
 * Reads larger than @p max_buffer are served without hedging, directly into the
 * caller's buffer, so a large read cannot multiply memory use.
 */
class HedgedObjectReadSource : public ObjectReadSource {
 public:
  using ChildFactory =
      std::function<StatusOr<std::unique_ptr<ObjectReadSource>>(
          std::int64_t current_offset, std::optional<std::int64_t> generation)>;
  using SimpleChildFactory =
      std::function<StatusOr<std::unique_ptr<ObjectReadSource>>()>;

  HedgedObjectReadSource(std::shared_ptr<ThreadPool> read_pool,
                         std::shared_ptr<HedgingThreadPool> hedge_pool,
                         ChildFactory child_factory,
                         std::chrono::milliseconds delay, int max_hedges,
                         std::size_t max_buffer, std::int64_t current_offset,
                         OffsetDirection offset_direction,
                         std::optional<std::int64_t> generation);

  HedgedObjectReadSource(std::shared_ptr<ThreadPool> read_pool,
                         std::shared_ptr<HedgingThreadPool> hedge_pool,
                         ChildFactory child_factory,
                         std::chrono::milliseconds delay, int max_hedges,
                         std::size_t max_buffer);

  HedgedObjectReadSource(std::shared_ptr<ThreadPool> read_pool,
                         std::shared_ptr<HedgingThreadPool> hedge_pool,
                         SimpleChildFactory child_factory,
                         std::chrono::milliseconds delay, int max_hedges,
                         std::size_t max_buffer);

  ~HedgedObjectReadSource() override = default;

  bool IsOpen() const override;
  StatusOr<HttpResponse> Close() override;
  StatusOr<ReadSourceResult> Read(char* buf, std::size_t n) override;

 private:
  void UpdateState(StatusOr<ReadSourceResult> const& result);

  std::shared_ptr<ThreadPool> read_pool_;
  std::shared_ptr<HedgingThreadPool> hedge_pool_;
  ChildFactory child_factory_;
  std::chrono::milliseconds delay_;
  int max_hedges_;
  std::size_t max_buffer_;

  std::int64_t current_offset_;
  OffsetDirection offset_direction_;
  std::optional<std::int64_t> generation_;
  bool is_gunzipped_ = false;

  std::unique_ptr<char[]> primary_buffer_;
  std::size_t primary_buffer_capacity_ = 0;

  std::unique_ptr<ObjectReadSource> active_child_;
  bool is_closed_ = false;
};

}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_STORAGE_INTERNAL_HEDGED_OBJECT_READ_SOURCE_H
