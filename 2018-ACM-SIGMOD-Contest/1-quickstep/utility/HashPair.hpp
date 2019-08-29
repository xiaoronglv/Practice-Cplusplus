#ifndef PROJECT_UTILITY_HASH_PAIR_HPP_
#define PROJECT_UTILITY_HASH_PAIR_HPP_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <type_traits>
#include <utility>

namespace project {

inline std::size_t CombineHashes(const std::size_t first_hash,
                                 const std::size_t second_hash) {
    return first_hash
        ^ (second_hash + 0x9e3779b9u + (first_hash << 6) + (first_hash >> 2));
}

}  // namespace project

namespace std {

template <typename FirstT, typename SecondT>
struct hash<pair<FirstT, SecondT>> {
  size_t operator()(const pair<FirstT, SecondT> &arg) const {
    size_t first_hash = hash<FirstT>()(arg.first);
    size_t second_hash = hash<SecondT>()(arg.second);
    return ::project::CombineHashes(first_hash, second_hash);
  }
};

}  // namespace std

#endif  // PROJECT_UTILITY_HASH_PAIR_HPP_
