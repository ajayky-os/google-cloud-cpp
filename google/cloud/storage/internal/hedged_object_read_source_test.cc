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
#include "google/cloud/storage/internal/object_read_source.h"
#include "google/cloud/storage/internal/dynamic_hedge_threshold_manager.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace google {
namespace cloud {
namespace storage {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace internal {
namespace {

using ::testing::_;
using ::testing::Return;

class MockObjectReadSource : public ObjectReadSource {
 public:
  MOCK_METHOD(bool, IsOpen, (), (const, override));
  MOCK_METHOD(StatusOr<HttpResponse>, Close, (), (override));
  MOCK_METHOD(StatusOr<ReadSourceResult>, Read, (char*, std::size_t), (override));
};

TEST(HedgedObjectReadSourceTest, HedgedRead) {
  auto pool = std::make_shared<HedgingThreadPool>(1, 0.0, 0.0, 0);

  auto manager = std::make_shared<DynamicHedgeThresholdManager>();

  auto factory = []() -> StatusOr<std::unique_ptr<ObjectReadSource>> {
    auto mock = std::make_unique<MockObjectReadSource>();
    EXPECT_CALL(*mock, Read(_, _))
        .WillOnce(Return(ReadSourceResult{10, HttpResponse{200, "payload", {}}}));
    return std::unique_ptr<ObjectReadSource>(std::move(mock));
  };

  ReadObjectRangeRequest request("bucket", "object");
  
  HedgedObjectReadSource source(
      pool, manager, request, factory, 1.2,
      std::chrono::milliseconds(500), 
      2 // max_hedges
  );

  std::vector<char> buffer(100);
  auto result = source.Read(buffer.data(), buffer.size());
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->bytes_received, 10);
}

}  // namespace
}  // namespace internal
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace storage
}  // namespace cloud
}  // namespace google
