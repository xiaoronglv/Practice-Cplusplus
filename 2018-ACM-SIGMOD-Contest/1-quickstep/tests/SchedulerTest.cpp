#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <iostream>

#include "scheduler/Scheduler.hpp"
#include "scheduler/Task.hpp"
#include "utility/Range.hpp"
#include "utility/EventProfiler.hpp"

#include "glog/logging.h"
#include "gflags/gflags.h"

using namespace project;  // NOLINT[build/namespaces]
using std::chrono_literals::operator""s;

static double calcSubLogSum(const Range &range) {
  auto *container = simple_profiler.getContainer();
  container->startEvent("calc");
  double sum = 0;
  for (std::size_t i = range.begin(); i < range.end(); ++i) {
    sum += std::log(i);
  }
  container->endEvent("calc");
  return sum;
}

static void calcLogSum(const std::uint64_t N, Scheduler *scheduler) {
  std::shared_ptr<std::atomic<double>> sum =
      std::make_shared<std::atomic<double>>(0);

  auto calc_task = [sum, N](Task *ctx) {
    const RangeSplitter splitter =
        RangeSplitter::CreateWithPartitionLength(1, N+1, 10000000);
    for (const auto range : splitter) {
      ctx->spawnLambdaTask([sum, range] {
        double subsum = calcSubLogSum(range);
        double value = sum->load(std::memory_order_relaxed);
        while (!sum->compare_exchange_weak(value, value + subsum)) {}
      });
    }
  };

  auto print_task = [N, sum](Task *ctx) {
    std::cout << "[" << (N / 100000000) << "] -- "
              << sum->load(std::memory_order_relaxed) << std::endl;
  };

  scheduler->addTask(CreateTaskChain(CreateLambdaTask(calc_task),
                                     CreateLambdaTask(print_task)));
}

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto *container = simple_profiler.getContainer();
  container->startEvent("overall");

  Scheduler &scheduler = Scheduler::GlobalInstance();
  scheduler.start();

  for (int i = 0; i < 10; ++i) {
    calcLogSum(100000000 * i, &scheduler);
  }

  scheduler.join();
  container->endEvent("overall");

  simple_profiler.summarizeToStream(std::cout);

  return 0;
}
