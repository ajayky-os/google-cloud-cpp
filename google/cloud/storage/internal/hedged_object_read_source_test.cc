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

#include "google/cloud/storage/internal/hedged_object_read_source.h"
#include "google/cloud/storage/testing/mock_client.h"
#include "google/cloud/testing_util/status_matchers.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {
namespace {

using ::google::cloud::storage::testing::MockObjectReadSource;
using ::google::cloud::testing_util::IsOk;
using ::google::cloud::testing_util::StatusIs;
using ::testing::Eq;
using ::testing::NotNull;
using ::testing::Return;

// Large enough that no test read is treated as oversized.
auto constexpr kUnlimitedBuffer = std::size_t{1} << 30;

std::shared_ptr<HedgingThreadPool> MakeUnlimitedPool() {
  return std::make_shared<HedgingThreadPool>(
      /*max_threads=*/4, /*rate_limit=*/0.0, /*capacity=*/0.0,
      /*max_concurrent=*/0);
}

ReadSourceResult MakeReadResult(std::string const& payload) {
  auto result =
      ReadSourceResult{payload.size(), HttpResponse{HttpStatusCode::kOk,
                                                    /*payload=*/{},
                                                    /*headers=*/{}}};
  return result;
}

TEST(HedgedObjectReadSourceTest, PrimaryWins) {
  auto factory = []() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    auto mock = std::make_unique<MockObjectReadSource>();
    EXPECT_CALL(*mock, Read).WillOnce([](char* buf, std::size_t) {
      std::string const payload = "payload";
      std::copy(payload.begin(), payload.end(), buf);
      return MakeReadResult(payload);
    });
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(500),
                                /*max_hedges=*/2, kUnlimitedBuffer);

  std::vector<char> buffer(100);
  auto result = source.Read(buffer.data(), buffer.size());
  ASSERT_THAT(result, IsOk());
  EXPECT_THAT(result->bytes_received, Eq(7));
  EXPECT_THAT(std::string(buffer.data(), result->bytes_received),
              Eq("payload"));
}

TEST(HedgedObjectReadSourceTest, SubsequentReadsContinueOnWinner) {
  // The factory must be called exactly once: after the open race is decided,
  // reads must continue on the winning child without creating new children,
  // otherwise the stream would restart at the wrong offset.
  auto factory_calls = std::make_shared<std::atomic<int>>(0);
  auto factory =
      [factory_calls]() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    ++*factory_calls;
    auto mock = std::make_unique<MockObjectReadSource>();
    EXPECT_CALL(*mock, Read)
        .WillOnce(Return(MakeReadResult("chunk-1")))
        .WillOnce(Return(MakeReadResult("chunk-2")));
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(500),
                                /*max_hedges=*/2, kUnlimitedBuffer);

  std::vector<char> buffer(100);
  EXPECT_THAT(source.Read(buffer.data(), buffer.size()), IsOk());
  EXPECT_THAT(source.Read(buffer.data(), buffer.size()), IsOk());
  EXPECT_THAT(factory_calls->load(), Eq(1));
}

TEST(HedgedObjectReadSourceTest, HedgeWinsWhenPrimaryStalls) {
  // The primary blocks until the end of the test, the hedge answers
  // immediately. The read must complete with the hedge's data, and the
  // (losing) primary must be closed once it completes.
  auto unblock_primary = std::make_shared<std::promise<void>>();
  auto primary_closed = std::make_shared<std::promise<void>>();
  auto calls = std::make_shared<std::atomic<int>>(0);
  auto factory = [unblock_primary, primary_closed,
                  calls]() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    auto mock = std::make_unique<MockObjectReadSource>();
    if (++*calls == 1) {
      EXPECT_CALL(*mock, Read).WillOnce([unblock_primary](char*, std::size_t) {
        unblock_primary->get_future().get();
        return MakeReadResult("slow");
      });
      EXPECT_CALL(*mock, Cancel).Times(1);
      EXPECT_CALL(*mock, Close).WillOnce([primary_closed]() {
        primary_closed->set_value();
        return make_status_or(HttpResponse{HttpStatusCode::kOk, {}, {}});
      });
    } else {
      EXPECT_CALL(*mock, Read).WillOnce(Return(MakeReadResult("hedge")));
    }
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  auto source = std::make_unique<HedgedObjectReadSource>(
      MakeUnlimitedPool(), factory, std::chrono::milliseconds(1),
      /*max_hedges=*/2, kUnlimitedBuffer);

  std::vector<char> buffer(100);
  auto result = source->Read(buffer.data(), buffer.size());
  ASSERT_THAT(result, IsOk());
  EXPECT_THAT(result->bytes_received, Eq(5));

  unblock_primary->set_value();
  primary_closed->get_future().get();
}

TEST(HedgedObjectReadSourceTest, PrimaryOpenErrorPropagates) {
  auto factory = []() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    return Status(StatusCode::kPermissionDenied, "uh-oh");
  };

  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(500),
                                /*max_hedges=*/2, kUnlimitedBuffer);

  std::vector<char> buffer(100);
  EXPECT_THAT(source.Read(buffer.data(), buffer.size()),
              StatusIs(StatusCode::kPermissionDenied));
}

TEST(HedgedObjectReadSourceTest, CloseWithoutReadSucceeds) {
  auto factory = []() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    return Status(StatusCode::kUnimplemented, "never called");
  };
  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(500),
                                /*max_hedges=*/2, kUnlimitedBuffer);
  EXPECT_TRUE(source.IsOpen());
  EXPECT_THAT(source.Close(), IsOk());
}

TEST(HedgedObjectReadSourceTest, CloseBeforeRead) {
  auto pool = std::make_shared<HedgingThreadPool>(1, 0.0, 0.0, 0);
  auto factory = []() {
    return std::unique_ptr<ObjectReadSource>(
        std::make_unique<MockObjectReadSource>());
  };
  HedgedObjectReadSource source(pool, factory, std::chrono::milliseconds(10), 2,
                                kUnlimitedBuffer);
  EXPECT_TRUE(source.IsOpen());
  EXPECT_STATUS_OK(source.Close());
  EXPECT_FALSE(source.IsOpen());
  auto const res = source.Read(nullptr, 1024);
  EXPECT_TRUE(res.ok());
  EXPECT_EQ(res->bytes_received, 0);
}

TEST(HedgedObjectReadSourceTest, OversizedReadIsNotHedged) {
  // A read larger than the limit must open exactly one child and read into the
  // caller's buffer, with no racing attempts to stage copies of the data.
  auto calls = std::make_shared<std::atomic<int>>(0);
  auto factory = [calls]() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    ++*calls;
    auto mock = std::make_unique<MockObjectReadSource>();
    EXPECT_CALL(*mock, Read).WillOnce([](char* buf, std::size_t) {
      std::string const payload = "direct";
      std::copy(payload.begin(), payload.end(), buf);
      return MakeReadResult(payload);
    });
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  // A zero delay would let a hedge start immediately if the limit were not
  // honored, so any race would be observable as extra factory calls.
  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(0),
                                /*max_hedges=*/2, /*max_buffer=*/8);

  std::vector<char> buffer(64);
  auto result = source.Read(buffer.data(), buffer.size());
  ASSERT_THAT(result, IsOk());
  EXPECT_THAT(result->bytes_received, Eq(6));
  EXPECT_THAT(std::string(buffer.data(), result->bytes_received), Eq("direct"));
  EXPECT_THAT(calls->load(), Eq(1));
}

TEST(HedgedObjectReadSourceTest, OversizedReadPropagatesOpenError) {
  auto factory = []() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    return Status(StatusCode::kPermissionDenied, "uh-oh");
  };

  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(0),
                                /*max_hedges=*/2, /*max_buffer=*/8);

  std::vector<char> buffer(64);
  EXPECT_THAT(source.Read(buffer.data(), buffer.size()),
              StatusIs(StatusCode::kPermissionDenied));
}

TEST(HedgedObjectReadSourceTest, SubsequentReadsIgnoreBufferLimit) {
  // The limit only decides whether the *open* is hedged. Once a child exists,
  // reads of any size continue on it without staging buffers.
  auto calls = std::make_shared<std::atomic<int>>(0);
  auto factory = [calls]() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    ++*calls;
    auto mock = std::make_unique<MockObjectReadSource>();
    EXPECT_CALL(*mock, Read)
        .WillOnce(Return(MakeReadResult("small")))
        .WillOnce(Return(MakeReadResult("large")));
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(500),
                                /*max_hedges=*/2, /*max_buffer=*/64);

  std::vector<char> small(8);
  EXPECT_THAT(source.Read(small.data(), small.size()), IsOk());
  // Well past the limit, but the winner is already open.
  std::vector<char> large(4096);
  EXPECT_THAT(source.Read(large.data(), large.size()), IsOk());
  EXPECT_THAT(calls->load(), Eq(1));
}

TEST(HedgedObjectReadSourceTest, LosingAttemptCancelledWhenWinnerResolves) {
  // The primary blocks in Read(). The hedge answers immediately.
  // When the hedge wins, the primary mock must have Cancel() called immediately.
  auto unblock_primary = std::make_shared<std::promise<void>>();
  auto primary_cancelled = std::make_shared<std::promise<void>>();
  auto primary_closed = std::make_shared<std::promise<void>>();
  auto calls = std::make_shared<std::atomic<int>>(0);

  HedgedObjectReadSource::ChildFactory factory =
      [unblock_primary, primary_cancelled, primary_closed,
       calls](std::shared_ptr<std::atomic<bool>> const& token)
      -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    auto mock = std::make_unique<MockObjectReadSource>();
    if (++*calls == 1) {
      EXPECT_CALL(*mock, Read)
          .WillOnce([unblock_primary, token](char*, std::size_t) {
            unblock_primary->get_future().get();
            EXPECT_TRUE(token->load(std::memory_order_relaxed));
            return MakeReadResult("slow");
          });
      EXPECT_CALL(*mock, Cancel).WillOnce([primary_cancelled]() {
        primary_cancelled->set_value();
      });
      EXPECT_CALL(*mock, Close).WillOnce([primary_closed]() {
        primary_closed->set_value();
        return make_status_or(HttpResponse{HttpStatusCode::kOk, {}, {}});
      });
    } else {
      EXPECT_CALL(*mock, Read).WillOnce(Return(MakeReadResult("hedge")));
    }
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  auto source = std::make_unique<HedgedObjectReadSource>(
      MakeUnlimitedPool(), factory, std::chrono::milliseconds(1),
      /*max_hedges=*/2, kUnlimitedBuffer);

  std::vector<char> buffer(100);
  StatusOr<ReadSourceResult> result =
      source->Read(buffer.data(), buffer.size());
  ASSERT_THAT(result, IsOk());
  EXPECT_THAT(result->bytes_received, Eq(5));

  // The winning hedge resolved the race, which should have called Cancel()
  // on the losing primary attempt even while it was blocked.
  primary_cancelled->get_future().get();

  unblock_primary->set_value();
  primary_closed->get_future().get();
}

TEST(HedgedObjectReadSourceTest, CancellationTokenPassedToChildFactory) {
  auto tokens =
      std::make_shared<std::vector<std::shared_ptr<std::atomic<bool>>>>();
  std::mutex mu;

  HedgedObjectReadSource::ChildFactory factory =
      [tokens, &mu](std::shared_ptr<std::atomic<bool>> const& token)
      -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    {
      std::lock_guard<std::mutex> lk(mu);
      tokens->push_back(token);
    }
    auto mock = std::make_unique<MockObjectReadSource>();
    EXPECT_CALL(*mock, Read).WillOnce(Return(MakeReadResult("data")));
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(500),
                                /*max_hedges=*/2, kUnlimitedBuffer);

  std::vector<char> buffer(100);
  StatusOr<ReadSourceResult> result = source.Read(buffer.data(), buffer.size());
  ASSERT_THAT(result, IsOk());

  std::lock_guard<std::mutex> lk(mu);
  ASSERT_THAT(tokens->size(), Eq(1));
  EXPECT_THAT((*tokens)[0], NotNull());
  EXPECT_FALSE((*tokens)[0]->load());
}

TEST(HedgedObjectReadSourceTest, CancelMethodCancelsActiveChildrenAndRace) {
  auto read_started = std::make_shared<std::promise<void>>();
  auto unblock_read = std::make_shared<std::promise<void>>();
  auto cancelled = std::make_shared<std::promise<void>>();

  HedgedObjectReadSource::ChildFactory factory =
      [read_started, unblock_read, cancelled](
          std::shared_ptr<std::atomic<bool>> const&)
      -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    auto mock = std::make_unique<MockObjectReadSource>();
    EXPECT_CALL(*mock, Read)
        .WillOnce([read_started, unblock_read](char*, std::size_t) {
          read_started->set_value();
          unblock_read->get_future().get();
          return MakeReadResult("data");
        });
    EXPECT_CALL(*mock, Cancel).WillOnce([cancelled]() {
      cancelled->set_value();
    });
    EXPECT_CALL(*mock, Close)
        .WillRepeatedly(Return(HttpResponse{HttpStatusCode::kOk, {}, {}}));
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  auto source = std::make_unique<HedgedObjectReadSource>(
      MakeUnlimitedPool(), factory, std::chrono::milliseconds(500),
      /*max_hedges=*/1, kUnlimitedBuffer);

  std::vector<char> buffer(100);
  auto read_async = std::async(std::launch::async, [&]() {
    return source->Read(buffer.data(), buffer.size());
  });

  read_started->get_future().get();
  source->Cancel();

  cancelled->get_future().get();
  unblock_read->set_value();

  (void)read_async.get();
}

TEST(HedgedObjectReadSourceTest, CancelBeforeReadFailsImmediately) {
  HedgedObjectReadSource::ChildFactory factory =
      [](std::shared_ptr<std::atomic<bool>> const&)
      -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    return Status(StatusCode::kUnimplemented, "should not be called");
  };

  HedgedObjectReadSource source(MakeUnlimitedPool(), factory,
                                std::chrono::milliseconds(500),
                                /*max_hedges=*/2, kUnlimitedBuffer);
  source.Cancel();

  std::vector<char> buffer(100);
  StatusOr<ReadSourceResult> result = source.Read(buffer.data(), buffer.size());
  EXPECT_THAT(result, StatusIs(StatusCode::kCancelled));
}

TEST(HedgedObjectReadSourceTest, HedgeReadFailureDoesNotAbortPrimary) {
  auto primary_started = std::make_shared<std::promise<void>>();
  auto unblock_primary = std::make_shared<std::promise<void>>();

  std::atomic<int> attempt_count{0};
  HedgedObjectReadSource::ChildFactory factory =
      [=, &attempt_count](std::shared_ptr<std::atomic<bool>> const&)
      -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    int const attempt = attempt_count++;
    auto mock = std::make_unique<MockObjectReadSource>();
    if (attempt == 0) {
      // Primary attempt: blocks until unblocked, then succeeds
      EXPECT_CALL(*mock, Read)
          .WillOnce([primary_started, unblock_primary](char* buf, std::size_t n) {
            primary_started->set_value();
            unblock_primary->get_future().get();
            std::string const data = "primary_data";
            std::memcpy(buf, data.data(), (std::min)(n, data.size()));
            return MakeReadResult(data);
          });
    } else {
      // Hedge attempt: fails immediately on Read()
      EXPECT_CALL(*mock, Read)
          .WillOnce(Return(Status(StatusCode::kUnavailable, "hedge read failed")));
    }
    EXPECT_CALL(*mock, Close)
        .WillRepeatedly(Return(HttpResponse{HttpStatusCode::kOk, {}, {}}));
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  auto source = std::make_unique<HedgedObjectReadSource>(
      MakeUnlimitedPool(), factory, std::chrono::milliseconds(10),
      /*max_hedges=*/1, kUnlimitedBuffer);

  std::vector<char> buffer(100);
  auto read_async = std::async(std::launch::async, [&]() {
    return source->Read(buffer.data(), buffer.size());
  });

  primary_started->get_future().get();
  // Wait for hedge to trigger and fail its read
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  // Unblock primary
  unblock_primary->set_value();

  StatusOr<ReadSourceResult> result = read_async.get();
  ASSERT_THAT(result, IsOk());
  EXPECT_THAT(std::string(buffer.data(), result->bytes_received),
              Eq("primary_data"));
}

}  // namespace
}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
