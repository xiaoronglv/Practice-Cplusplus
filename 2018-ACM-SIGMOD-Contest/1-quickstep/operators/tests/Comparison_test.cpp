#include "operators/expressions/Comparison.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include "operators/expressions/ScalarAttribute.hpp"
#include "operators/expressions/ScalarLiteral.hpp"
#include "storage/Attribute.hpp"
#include "storage/Relation.hpp"
#include "types/Type.hpp"
#include "utility/Range.hpp"

#include "gtest/gtest.h"

using std::make_unique;

namespace project {

namespace {

// r3.c2 in large.
const std::uint64_t kBase = 1u;
constexpr std::uint64_t kRange = 938495u - kBase + 1;

}  // namespace

class ComparisonTest : public ::testing::Test {
 protected:
  ComparisonTest()
      : type_(Type::GetInstance(kUInt64)),
        rel_(std::vector<const Type*>(1u, &type_), false),
        attr_(2, type_, &rel_),
        input_(kBase, kBase + kRange),
        exactness_(true) {}

  std::unique_ptr<Comparison> createComparison(const ComparisonType comparison_type,
                                               const std::uint64_t literal) {
    auto lhs = make_unique<ScalarAttribute>(&attr_);
    auto rhs = make_unique<ScalarLiteral>(literal, type_);
    return make_unique<Comparison>(comparison_type, lhs.release(), rhs.release());
  }

  const Type &type_;
  Relation rel_;
  const Attribute attr_;
  const Range input_;
  bool exactness_;
};

TEST_F(ComparisonTest, LessTestInRange) {
  const std::uint64_t kBoundary = kBase + 307742u;
  auto comparison = createComparison(ComparisonType::kLess, kBoundary);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(input_.begin(), output.begin());
  EXPECT_EQ(kBoundary, output.end());
  EXPECT_TRUE(exactness_);
}

TEST_F(ComparisonTest, LessTestSmallerThanBase) {
  auto comparison = createComparison(ComparisonType::kLess, kBase);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(0, output.size());
  EXPECT_TRUE(exactness_);
}

TEST_F(ComparisonTest, LessTestLargerThanMax) {
  auto comparison = createComparison(ComparisonType::kLess, kBase + kRange + 10);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(input_.begin(), output.begin());
  EXPECT_EQ(input_.end(), output.end());
  EXPECT_TRUE(exactness_);
}

TEST_F(ComparisonTest, GreaterTestInRange) {
  const std::uint64_t kBoundary = kBase + 193348u;
  auto comparison = createComparison(ComparisonType::kGreater, kBoundary);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(kBoundary + 1, output.begin());
  EXPECT_EQ(input_.end(), output.end());
  EXPECT_TRUE(exactness_);
}

TEST_F(ComparisonTest, GreaterTestLargerThanMax) {
  auto comparison = createComparison(ComparisonType::kGreater, kBase + kRange + 10);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(0, output.size());
  EXPECT_TRUE(exactness_);
}

TEST_F(ComparisonTest, GreaterTestSmallerThanBase) {
  auto comparison = createComparison(ComparisonType::kGreater, kBase);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(kBase + 1, output.begin());
  EXPECT_EQ(input_.end(), output.end());
  EXPECT_TRUE(exactness_);
}

TEST_F(ComparisonTest, EqualTestInRange) {
  const std::uint64_t kBoundary = kBase + 274018u;
  auto comparison = createComparison(ComparisonType::kEqual, kBoundary);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(kBoundary, output.begin());
  EXPECT_EQ(kBoundary + 1, output.end());
  EXPECT_TRUE(exactness_);
}

TEST_F(ComparisonTest, EqualTestLessThanBase) {
  auto comparison = createComparison(ComparisonType::kEqual, kBase - 1);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(0, output.size());
  EXPECT_TRUE(exactness_);
}

TEST_F(ComparisonTest, EqualTestLargerThanMax) {
  auto comparison = createComparison(ComparisonType::kEqual, kBase + kRange + 10);
  const Range output = comparison->reduceRange(input_, &exactness_);

  EXPECT_EQ(0, output.size());
  EXPECT_TRUE(exactness_);
}

}  // namespace project
