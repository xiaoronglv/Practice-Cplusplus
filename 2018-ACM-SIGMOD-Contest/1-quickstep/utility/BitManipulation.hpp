#ifndef PROJECT_UTILITY_BIT_MANIPULATION_HPP_
#define PROJECT_UTILITY_BIT_MANIPULATION_HPP_

#include <cstdint>

#include "ProjectConfig.h"

namespace project {

template <typename UIntType>
inline int PopulationCount(UIntType word) {
  int count = 0;
  while (word) {
    if (word & 0x1U) {
      ++count;
    }
    word >>= 1;
  }
  return count;
}

#ifdef PROJECT_HAVE_BUILTIN_POPCOUNT
template <>
inline int PopulationCount<std::uint8_t>(std::uint8_t word) {
  return __builtin_popcount(word);
}

template <>
inline int PopulationCount<std::uint16_t>(std::uint16_t word) {
  return __builtin_popcount(word);
}

template <>
inline int PopulationCount<unsigned>(unsigned word) {
  return __builtin_popcount(word);
}

template <>
inline int PopulationCount<unsigned long>(unsigned long word) {  // NOLINT(runtime/int)
  return __builtin_popcountl(word);
}

template <>
inline int PopulationCount<unsigned long long>(unsigned long long word) {  // NOLINT(runtime/int)
  return __builtin_popcountll(word);
}
#endif


template <typename UIntType>
inline int LeadingZeroCount(UIntType word) {
  if (word) {
    constexpr UIntType maxone = static_cast<UIntType>(0x1) << ((sizeof(UIntType) << 3) - 1);
    int count = 0;
    while (!(word & maxone)) {
      ++count;
      word <<= 1;
    }
    return count;
  } else {
    return sizeof(UIntType) << 3;
  }
}

#ifdef PROJECT_HAVE_BUILTIN_CLZ
template <>
inline int LeadingZeroCount<std::uint8_t>(std::uint8_t word) {
  return __builtin_clz(word) - 24;
}

template <>
inline int LeadingZeroCount<std::uint16_t>(std::uint16_t word) {
  return __builtin_clz(word) - 16;
}

template <>
inline int LeadingZeroCount<unsigned>(unsigned word) {
  return __builtin_clz(word);
}

template <>
inline int LeadingZeroCount<unsigned long>(unsigned long word) {  // NOLINT[runtime/int]
  return __builtin_clzl(word);
}

template <>
inline int LeadingZeroCount<unsigned long long>(unsigned long long word) {  // NOLINT[runtime/int]
  return __builtin_clzll(word);
}
#endif


template <typename UIntType>
inline int TrailingZeroCount(UIntType word) {
  if (word) {
    int count = 0;
    while (!(word & 0x1U)) {
      ++count;
      word >>= 1;
    }
    return count;
  } else {
    return sizeof(UIntType) << 3;
  }
}

#ifdef PROJECT_HAVE_BUILTIN_CTZ
template <>
inline int TrailingZeroCount<std::uint8_t>(std::uint8_t word) {
  return __builtin_ctz(word);
}

template <>
inline int TrailingZeroCount<std::uint16_t>(std::uint16_t word) {
  return __builtin_ctz(word);
}

template <>
inline int TrailingZeroCount<unsigned>(unsigned word) {
  return __builtin_ctz(word);
}

template <>
inline int TrailingZeroCount<unsigned long>(unsigned long word) {  // NOLINT(runtime/int)
  return __builtin_ctzl(word);
}

template <>
inline int TrailingZeroCount<unsigned long long>(unsigned long long word) {  // NOLINT(runtime/int)
  return __builtin_ctzll(word);
}
#endif


template <typename UIntType>
inline int MostSignificantBit(UIntType word) {
  return (sizeof(UIntType) << 3) - 1 - LeadingZeroCount<UIntType>(word);
}

}  // namespace project

#endif  // PROJECT_UTILITY_BIT_MANIPULATION_HPP_
