#ifndef PROJECT_OPTIMIZER_SCALAR_HPP_
#define PROJECT_OPTIMIZER_SCALAR_HPP_

#include <memory>
#include <unordered_map>

#include "optimizer/ExprId.hpp"
#include "optimizer/Expression.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class Attribute;
class Scalar;

namespace optimizer {

class AttributeReference;
class Scalar;
typedef std::shared_ptr<const Scalar> ScalarPtr;

class Scalar : public Expression {
 public:
  virtual std::shared_ptr<const AttributeReference> getAttribute() const = 0;

  virtual ::project::Scalar* concretize(
      const std::unordered_map<ExprId, const Attribute*> &substitution_map) const = 0;

 protected:
  Scalar() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Scalar);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_SCALAR_HPP_
