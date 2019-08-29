#ifndef PROJECT_OPERATORS_SPECIALIZATIONS_KEY_COUNT_VECTOR_ACCUMULATOR_01_HPP_
#define PROJECT_OPERATORS_SPECIALIZATIONS_KEY_COUNT_VECTOR_ACCUMULATOR_01_HPP_

#include <atomic>
#include <cstddef>
#include <vector>

#include "utility/HashFilter.hpp"
#include "utility/Macros.hpp"

namespace project {

class Task;

namespace kcv {

class KeyCountVectorAccumulatorCommon;

void AccumulateX(Task *ctx, KeyCountVectorAccumulatorCommon *accumulator);

void AccumulateXX(Task *ctx, KeyCountVectorAccumulatorCommon *accumulator);


}  // namespace kcv
}  // namespace project

#endif  // PROJECT_OPERATORS_SPECIALIZATIONS_KEY_COUNT_VECTOR_ACCUMULATOR_01_HPP_
