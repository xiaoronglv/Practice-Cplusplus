#ifndef PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_ACCUMULATOR_HPP_
#define PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_ACCUMULATOR_HPP_

#include <atomic>
#include <vector>

#include "utility/HashFilter.hpp"
#include "utility/Macros.hpp"

namespace project {

struct MultiwayJoinContext;
class KeyCountVector;
class Range;
class Task;

class KeyCountVectorAccumulator {
 public:
  static void Invoke(Task *ctx,
                     MultiwayJoinContext *context,
                     const Range &domain,
                     std::vector<const KeyCountVector*> &&kcvs,
                     const HashFilter *lookahead_filter,
                     std::atomic<bool> *is_null);

 private:
  DISALLOW_COPY_AND_ASSIGN(KeyCountVectorAccumulator);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_ACCUMULATOR_HPP_
