#ifndef PROJECT_STORAGE_ATTRIBUTE_HPP_
#define PROJECT_STORAGE_ATTRIBUTE_HPP_

#include <string>

#include "storage/StorageTypedefs.hpp"
#include "types/Type.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class Relation;

class Attribute {
 public:
  Attribute(const attribute_id id,
            const Type &type,
            Relation *parent_relation)
      : id_(id),
        type_(&type),
        parent_relation_(parent_relation) {
    DCHECK(parent_relation_ != nullptr);
  }

  attribute_id id() const {
    return id_;
  }

  std::string getName() const {
    return "c" + std::to_string(id_);
  }

  const Type& getType() const {
    return *type_;
  }

  void setType(const Type &type) {
    type_ = &type;
  }

  const Relation& getParentRelation() const {
    return *parent_relation_;
  }

  Relation* getParentRelationMutable() const {
    return parent_relation_;
  }

 private:
  const attribute_id id_;
  const Type *type_;
  Relation *parent_relation_;

  DISALLOW_COPY_AND_ASSIGN(Attribute);
};

}  // namespace project

#endif  // PROJECT_STORAGE_ATTRIBUTE_HPP_
