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
using ::testing::Return;

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
                                /*max_hedges=*/2);

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
                                /*max_hedges=*/2);

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
      /*max_hedges=*/2);

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
                                /*max_hedges=*/2);

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
                                /*max_hedges=*/2);
  EXPECT_TRUE(source.IsOpen());
  EXPECT_THAT(source.Close(), IsOk());
}

}  // namespace
}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
