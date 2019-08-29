#ifndef PROJECT_OPTIMIZER_RULES_RULE_HPP_
#define PROJECT_OPTIMIZER_RULES_RULE_HPP_

#include <memory>
#include <string>

#include "optimizer/Plan.hpp"
#include "utility/Macros.hpp"

namespace project {
namespace optimizer {

template <typename TreeType>
class Rule {
 private:
  typedef std::shared_ptr<const TreeType> TreeNodePtr;

 public:
  Rule() {}

  virtual ~Rule() {}

  virtual std::string getName() const = 0;

  virtual TreeNodePtr apply(const TreeNodePtr &input) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(Rule);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_RULES_RULE_HPP_
