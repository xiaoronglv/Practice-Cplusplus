#include "workload/Workload.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "operators/utility/KeyCountVector.hpp"
#include "optimizer/Optimizer.hpp"
#include "optimizer/QueryHandle.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/Task.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/ColumnStoreBlock.hpp"
#include "storage/ColumnVector.hpp"
#include "storage/Database.hpp"
#include "storage/ForeignKeyIndex.hpp"
#include "storage/IndexManager.hpp"
#include "storage/PrimaryKeyIndex.hpp"
#include "storage/Relation.hpp"
#include "storage/RelationStatistics.hpp"
#include "storage/StorageTypedefs.hpp"
#include "threading/SynchronizationLock.hpp"
#include "types/Type.hpp"
#include "utility/EventProfiler.hpp"
#include "utility/ScopedArray.hpp"
#include "workload/TableAnalyzer.hpp"

#include "gflags/gflags.h"

namespace O = ::project::optimizer;

namespace project {

DEFINE_bool(print_stats, false, "Print table statistics");

Workload::Workload()
    : query_id_counter_(0) {
}

void Workload::loadTables(const std::vector<std::string> &filenames,
                          Scheduler *scheduler) {
  std::vector<std::unique_ptr<Relation>> relations(filenames.size());
  SynchronizationLock sync_lock;

  Task *task = CreateTaskChain(
      CreateLambdaTask([this, &relations, &filenames](Task *ctx) {
        for (std::size_t i = 0; i < filenames.size(); ++i) {
          ctx->spawnLambdaTask([this, &relations, &filenames, i](Task *ctx) {
            std::unique_ptr<Relation> &relation = relations[i];
            relation.reset(this->loadTable(filenames[i]));
          });
        }
      }),
      CreateLambdaTask([this, &relations](Task *ctx) {
        for (std::unique_ptr<Relation> &relation : relations) {
          this->database_.addRelation(relation.release());
        }
        TableAnalyzer analyzer;
        analyzer.analyzeDatabase(ctx, &this->database_);
      }),
      CreateLambdaTask([&sync_lock] {
        sync_lock.release();
      })
  );  // NOLINT[whitespace/parens]

  task->setProfiling(true);
  task->setCascade(true);
  task->setTaskMajorId(0);

  scheduler->addTask(task);

  sync_lock.wait();

  if (FLAGS_print_stats) {
    for (std::size_t i = 0; i < database_.getNumRelations(); ++i) {
      std::cerr << "[Relation " << i << "] "
                << database_.getRelation(i).getBriefSummary() << "\n";
    }
    std::cerr << "\n" << database_.getBriefSummary() << "\n";
  }
}

std::vector<std::string> Workload::evaluateQueries(
    const std::vector<std::string> &queries) {
  Scheduler &scheduler = Scheduler::GlobalInstance();

  std::vector<std::string> results(queries.size());
  std::vector<std::unique_ptr<O::QueryHandle>> handles(queries.size());
  SynchronizationLock sync_lock;

  scheduler.addTask(CreateTaskChain(
      CreateLambdaTask([this, &queries, &handles](Task *ctx) {
        for (std::size_t i = 0; i < queries.size(); ++i) {
          ctx->spawnLambdaTask([this, &queries, &handles, i](Task *internal) {
            std::unique_ptr<O::QueryHandle> handle =
                std::make_unique<O::QueryHandle>(this->query_id_counter_ + i);

            O::Optimizer optimizer(&this->database_);
            optimizer.generateQueryHandle(queries[i], handle.get());

            if (handle->getExecutionPlan()->size() != 0) {
              internal->spawnTask(handle->releaseExecutionPlan());
            }
            handles[i] = std::move(handle);
          });
        }
      }),
      CreateLambdaTask([&results, &handles, &sync_lock] {
        for (std::size_t i = 0; i < results.size(); ++i) {
          results[i] = handles[i]->getResultString();
        }
        sync_lock.release();
      })
  ));  // NOLINT[whitespace/parens]

  sync_lock.wait();
  query_id_counter_ += queries.size();

  return results;
}

Relation* Workload::loadTable(const std::string &filename) const {
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd == -1) {
    throw std::runtime_error("Cannot open " + filename);
  }

  // Obtain file size.
  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    throw std::runtime_error("fstat failed");
  }

  const auto length = sb.st_size;
  char *addr = static_cast<char*>(
      mmap(nullptr, length, PROT_READ, MAP_PRIVATE, fd, 0u));
  if (addr == MAP_FAILED) {
    throw std::runtime_error(
        "Cannot mmap " + filename + " of length " + std::to_string(length));
  }

  if (length < 16) {
    throw std::runtime_error(
        "Relation file " + filename + " does not contain a valid header");
  }

  const std::size_t num_tuples = *reinterpret_cast<const std::uint64_t*>(addr);
  addr += sizeof(std::uint64_t);

  const std::size_t num_columns = *reinterpret_cast<const std::uint64_t*>(addr);
  addr += sizeof(std::uint64_t);

  std::vector<const Type*> column_types(num_columns, &Type::GetInstance(kUInt64));
  std::unique_ptr<Relation> relation =
      std::make_unique<Relation>(column_types, false /* is_temporary */);

  std::unique_ptr<ColumnStoreBlock> block = std::make_unique<ColumnStoreBlock>();
  for (std::size_t i = 0; i < num_columns; ++i) {
    std::unique_ptr<ColumnVector> column =
        std::make_unique<ColumnVectorImpl<std::uint64_t>>(
            num_tuples, reinterpret_cast<std::uint64_t*>(addr), false /* owns */);
    block->addColumn(column.release());
    addr += num_tuples * sizeof(std::uint64_t);
  }
  relation->addBlock(block.release());

  return relation.release();
}

std::size_t Workload::getTotalNumTuples() const {
  return database_.getTotalNumTuples();
}

}  // namespace project
