#ifndef PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_CHAIN_HPP_
#define PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_CHAIN_HPP_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

#include "utility/Macros.hpp"

namespace project {

template <typename ...CountVectors>
class CountVectorChain {
 public:
  explicit CountVectorChain(const CountVectors *...cvs)
      : cvs_(cvs...) {}

  inline std::uint64_t at(const std::uint64_t value) const {
    return atInternal<sizeof...(CountVectors)-1, false>(value);
  }

  inline std::uint64_t atChecked(const std::uint64_t value) const {
    return atInternal<sizeof...(CountVectors)-1, true>(value);
  }

 private:
  template <std::size_t I, bool check>
  inline std::uint64_t atInternal(const std::uint64_t value,
                                  std::enable_if_t<I != 0> * = 0) const {
    const std::uint64_t count =
        std::get<I>(cvs_)-> template atConditional<check>(value);
    if (count == 0) {
      return 0;
    }
    return count * atInternal<I-1, check>(value);
  }

  template <std::size_t I, bool check>
  inline std::uint64_t atInternal(const std::uint64_t value,
                                  std::enable_if_t<I == 0> * = 0) const {
    return std::get<0>(cvs_)->template atConditional<check>(value);
  }

  std::tuple<const CountVectors*...> cvs_;

  DISALLOW_COPY_AND_ASSIGN(CountVectorChain);
};

template <>
class CountVectorChain<> {
 public:
  CountVectorChain() {}

  inline std::uint64_t at(const std::uint64_t value) const {
    return 1;
  }

  inline std::uint64_t atChecked(const std::uint64_t value) const {
    return 1;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(CountVectorChain);
};

}  // namespace project

#endif  // PROJECT_OPERATORS_UTILITY_KEY_COUNT_VECTOR_CHAIN_HPP_
