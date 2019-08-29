#ifndef PROJECT_PARTITION_PARTITION_DESTINATION_BASE_HPP_
#define PROJECT_PARTITION_PARTITION_DESTINATION_BASE_HPP_

#include "utility/Macros.hpp"

namespace project {

class PartitionDestinationBase {
 public:
  virtual ~PartitionDestinationBase() {}

 protected:
  PartitionDestinationBase() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(PartitionDestinationBase);
};

}  // namespace project

#endif  // PROJECT_PARTITION_PARTITION_DESTINATION_BASE_HPP_
