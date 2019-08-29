#ifndef PROJECT_UTILITY_MEMORY_UTIL_HPP_
#define PROJECT_UTILITY_MEMORY_UTIL_HPP_

#include <memory>
#include <utility>
#include <vector>

namespace project {

template <typename T>
static inline std::shared_ptr<T> CreateShared(const T &object) {
  return std::make_shared<T>(object);
}

template <typename T>
static inline std::shared_ptr<T> CreateShared(T &&object) {
  return std::make_shared<T>(std::move(object));
}

template<class TargetType, class SourceContainer>
static inline std::vector<std::shared_ptr<const TargetType>> CastSharedPtrVector(
    const SourceContainer &source_ptrs) {
  std::vector<std::shared_ptr<const TargetType>> target_ptrs;
  target_ptrs.reserve(source_ptrs.size());
  for (const auto &ptr : source_ptrs) {
    target_ptrs.emplace_back(std::static_pointer_cast<const TargetType>(ptr));
  }
  return target_ptrs;
}

}  // namespace project

#endif  // PROJECT_UTILITY_MEMORY_UTIL_HPP_
