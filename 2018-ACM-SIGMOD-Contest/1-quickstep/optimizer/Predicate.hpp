#ifndef PROJECT_OPTIMIZER_PREDICATE_HPP_
#define PROJECT_OPTIMIZER_PREDICATE_HPP_

#include <memory>
#include <unordered_map>

#include "types/Type.hpp"
#include "optimizer/ExprId.hpp"
#include "optimizer/Expression.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

class Predicate;

namespace optimizer {

class Predicate;
typedef std::shared_ptr<const Predicate> PredicatePtr;

class Predicate : public Expression {
 public:
  const Type& getValueType() const override {
    // We don't need BoolType ...
    LOG(FATAL) << "Invalid call to Predicate::getValueType()";
  }

  virtual Range reduceRange(const ExprId expr_id,
                            const Range &input) const = 0;

  virtual ::project::Predicate* concretize(
      const std::unordered_map<ExprId, const Attribute*> &substitution_map) const = 0;

 protected:
  Predicate() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(Predicate);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_PREDICATE_HPP_
