#ifndef PROJECT_PARTITION_PARTITION_EXECUTOR_HPP_
#define PROJECT_PARTITION_PARTITION_EXECUTOR_HPP_

#include <memory>

#include "partition/PartitionDestination.hpp"
#include "scheduler/Task.hpp"
#include "utility/Macros.hpp"

namespace project {

template <typename Tuple, typename HashFunctor>
class PartitionExecutor {
 public:
  explicit PartitionExecutor(const HashFunctor &hash_functor)
      : hash_functor_(hash_functor) {
    output_ = std::make_unique<PartitionDestination<Tuple>>(
        hash_functor.getNumPartitions());
  }

  inline std::size_t getNumPartitions() const {
    return hash_functor_.getNumPartitions();
  }

  template <typename Accessor>
  inline void insert(const Accessor &accessor) {
    PartitionGroup<Tuple> *group = output_->allocateGroup();

    accessor.forEach([&](const auto &tuple) {
      group->append(hash_functor_(tuple), tuple);
    });
    output_->returnGroup(group);
  }

  template <typename Functor>
  inline void forEach(const std::size_t partition_id,
                      const Functor &functor) const {
    output_->forEach(partition_id, functor);
  }

  std::size_t count() const {
    return output_->count();
  }

  PartitionDestination<Tuple>* releaseOutput() {
    return output_.release();
  }

 private:
  const HashFunctor hash_functor_;
  std::unique_ptr<PartitionDestination<Tuple>> output_;

  DISALLOW_COPY_AND_ASSIGN(PartitionExecutor);
};

}  // namespace project

#endif  // PROJECT_PARTITION_PARTITION_EXECUTOR_HPP_
