#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <random>

#include "operators/aggregators/BuildKeyCountVector.hpp"
#include "operators/utility/KeyCountVector.hpp"
#include "partition/PartitionDestination.hpp"
#include "partition/PartitionExecutor.hpp"
#include "partition/RangeSegmentation.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/Task.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/GenericAccessor.hpp"
#include "types/Type.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"
#include "gflags/gflags.h"

namespace project {
namespace test {

class PartitionTest {
 public:
  using ColumnVectorType = ColumnVectorImpl<std::uint32_t>;
  using CountVectorType = KeyCountVectorImpl<std::uint8_t>;

  void naive() {
    auto *container = simple_profiler.getContainer();
    container->startEvent("naive");

    CountVectorType cv(kBase, kLength);
    for (std::size_t i = 0; i < kNumTuples; ++i) {
      cv.increaseCount(data_->at(i));
    }

    container->endEvent("naive");
  }

  void advanced() {
    auto *container = simple_profiler.getContainer();

    std::unique_ptr<ColumnAccessor> column(data_->createColumnAccessor());

    std::unique_ptr<KeyCountVector> kcv;

    std::unique_ptr<KeyCountVectorBuilder> builder(
        KeyCountVectorBuilder::Create(UInt32Type::Instance(),
                                      column->getNumTuples(),
                                      Range(kBase, kDomain+1),
                                      255,
                                      nullptr,
                                      &kcv));

    Scheduler &scheduler = Scheduler::GlobalInstance();

    scheduler.addTask(CreateTaskChain(
        CreateLambdaTask([&](Task *ctx) {
          const RangeSplitter splitter =
              RangeSplitter::CreateWithPartitionLength(
                  0, column->getNumTuples(), 100000);

          for (const Range range : splitter) {
            builder->accumulate(
                ctx,
                std::shared_ptr<ColumnAccessor>(column->createSubset(range)),
                nullptr);
          }
        }),
        CreateLambdaTask([&](Task *ctx) {
          builder->finalize(ctx);
        })));

    container->startEvent("advanced");

    scheduler.start();
    scheduler.join();

    container->endEvent("advanced");

    const CountVectorType *cv = static_cast<const CountVectorType*>(kcv.get());
    std::uint64_t sum = 0;
    for (std::uint32_t value = kBase; value <= kDomain; ++value) {
      sum += value * cv->at(value);
    }
    std::cerr << "sum = " << sum << "\n";
  }

  void run() {
    createColumn();

//    naive();
    advanced();

    simple_profiler.writeToStream(std::cerr);
    simple_profiler.summarizeToStream(std::cerr);
  }

 private:
  void createColumn() {
    std::mt19937 e(0);
    std::uniform_int_distribution<std::uint32_t> uniform_dist(kBase, kDomain);

    const std::size_t kOnePercent = kNumTuples / 100;
    data_ = std::make_unique<ColumnVectorType>(kNumTuples);
    for (std::size_t i = 0; i < kNumTuples; ++i) {
      if (i % kOnePercent == 0) {
        std::cerr << "Generating data ... " << (i / kOnePercent) << "%\r";
      }
      data_->appendValue(uniform_dist(e));
    }
    std::cerr << "Generating data ... done\n";
  }

  std::unique_ptr<ColumnVectorType> data_;

  const std::size_t kBase = 1000;
  const std::size_t kNumTuples = 30000000;
  const std::size_t kDomain = 41849153;
  const std::size_t kLength = kDomain - kBase + 1;
};

}  // namespace test
}  // namespace project

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  project::test::PartitionTest().run();

  return 0;
}
