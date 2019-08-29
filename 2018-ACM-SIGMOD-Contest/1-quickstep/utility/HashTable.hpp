#ifndef PROJECT_UTILITY_HASH_TABLE_HPP_
#define PROJECT_UTILITY_HASH_TABLE_HPP_

#include <cstddef>
#include <tuple>
#include <unordered_map>

#include "utility/Macros.hpp"

namespace project {

class HashTable {
 public:
  virtual ~HashTable() {}

 protected:
  HashTable() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(HashTable);
};

template <typename Key, typename ...Values>
class HashTableImpl : public HashTable {
 public:
  HashTableImpl() {}

  using Tuple = std::tuple<Values...>;
  using Bucket = typename std::unordered_multimap<Key, Tuple>::value_type;

  const std::unordered_multimap<Key, Tuple>& getTable() const {
    return table_;
  }

  std::unordered_multimap<Key, Tuple>* getTableMutable() {
    return &table_;
  }

 private:
  std::unordered_multimap<Key, Tuple> table_;

  DISALLOW_COPY_AND_ASSIGN(HashTableImpl);
};

}  // namespace project

#endif  // PROJECT_UTILITY_HASH_TABLE_HPP_
