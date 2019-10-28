#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "scheduler/Scheduler.hpp"
#include "scheduler/TaskStatistics.hpp"
#include "workload/Workload.hpp"
#include "utility/EventProfiler.hpp"

#include "glog/logging.h"
#include "gflags/gflags.h"

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto *container = project::simple_profiler.getContainer();

  project::Workload workload;
  std::string line;

  // Read relation file names from stdin.
  std::vector<std::string> relations;
  while (std::getline(std::cin, line)) {
    if (line == "Done") {
      break;
    }
    relations.emplace_back(line);
  }

  container->startEvent("Table load time");

  // A separate scheduler for loading tables.
  // For convenience of single-thread query evaluation benchmarking.
  std::unique_ptr<project::Scheduler> scheduler = 
      std::make_unique<project::Scheduler>(std::thread::hardware_concurrency());
  scheduler->start();

  workload.loadTables(relations, scheduler.get());

  scheduler->join();

  container->endEvent("Table load time");

//  project::TaskStatistics preprocessing_stats(*scheduler);

//  preprocessing_stats.printPerColumnPreprocessingToStream(std::cerr);
//  preprocessing_stats.summarizePerColumnPreprocessingToStream(std::cerr);

//  std::cerr << "Total # profiled tasks: "
//            << scheduler->getTotalNumTaskEvents() << "\n";

  scheduler.reset();

  project::Scheduler::GlobalInstance().start();

  std::size_t batch = 0;

  // Read queries from stdin.
  std::vector<std::string> queries;
  queries.reserve(16);
  while (std::getline(std::cin, line)) {
    if (line == "F") {
      if (batch == 0) {
        container->startEvent("Query run time");
      }

      const std::string batch_name = "b" + std::to_string(batch);
      container->startEvent(batch_name);

      std::vector<std::string> results = workload.evaluateQueries(queries);
      for (const std::string &result : results) {
        std::cout << result << std::endl;
      }
      queries.clear();
      ++batch;

      container->endEvent(batch_name);
    } else {
      queries.emplace_back(line);
    }
  }
  container->endEvent("Query run time");

//  project::simple_profiler.writeToStream(std::cerr);
  project::simple_profiler.summarizeToStream(std::cerr);

//  project::TaskStatistics query_stats(project::Scheduler::GlobalInstance());
//  query_stats.printPerQueryToStream(std::cerr);
//  query_stats.summarizePerQueryToStream(std::cerr);

//  std::cerr << "Total # profiled tasks: "
//            << project::Scheduler::GlobalInstance().getTotalNumTaskEvents()
//            << "\n";

  project::Scheduler::GlobalInstance().join();
}

