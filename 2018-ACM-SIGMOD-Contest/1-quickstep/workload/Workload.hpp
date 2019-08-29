#ifndef PROJECT_WORKLOAD_WORKLOAD_HPP_
#define PROJECT_WORKLOAD_WORKLOAD_HPP_

#include <string>
#include <vector>

#include "scheduler/Scheduler.hpp"
#include "storage/Database.hpp"
#include "storage/Relation.hpp"
#include "utility/Macros.hpp"

namespace project {

class Workload {
 public:
  Workload();

  void loadTables(const std::vector<std::string> &filenames,
                  Scheduler *scheduler);

  std::vector<std::string> evaluateQueries(const std::vector<std::string> &queries);

  std::size_t getTotalNumTuples() const;

 private:
  Relation* loadTable(const std::string &filename) const;

  Database database_;
  std::size_t query_id_counter_;

  DISALLOW_COPY_AND_ASSIGN(Workload);
};

}  // namespace project

#endif  // PROJECT_WORKLOAD_WORKLOAD_HPP_
