#ifndef PROJECT_STORAGE_STORAGE_TYPEDEFS_HPP_
#define PROJECT_STORAGE_STORAGE_TYPEDEFS_HPP_

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace project {

typedef int relation_id;
typedef int attribute_id;
typedef std::int32_t tuple_id;

constexpr attribute_id kInvalidAttributeID = -1;
constexpr tuple_id kInvalidTupleID = -1;

}  // namespace project

#endif  // PROJECT_STORAGE_STORAGE_TYPEDEFS_HPP_
