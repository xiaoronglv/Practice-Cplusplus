#ifndef PROJECT_OPTIMIZER_PROJECT_HPP_
#define PROJECT_OPTIMIZER_PROJECT_HPP_

#include <memory>
#include <string>
#include <vector>

#include "optimizer/AttributeReference.hpp"
#include "optimizer/Plan.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {
namespace optimizer {

class Project;
typedef std::shared_ptr<const Project> ProjectPtr;

class Project : public Plan {
 public:
  PlanType getPlanType() const override {
    return PlanType::kProject;
  }

  std::string getName() const override {
    return "Project";
  }

  std::vector<AttributeReferencePtr> getOutputAttributes() const override;

  std::vector<AttributeReferencePtr> getReferencedAttributes() const override;

  PlanPtr copyWithNewChildren(
      const std::vector<PlanPtr> &new_children) const override;

  const PlanPtr& input() const {
    return input_;
  }

  const std::vector<AttributeReferencePtr>& project_attributes() const {
    return project_attributes_;
  }

  static ProjectPtr Create(
      const PlanPtr &input,
      const std::vector<AttributeReferencePtr> &project_attributes) {
    return ProjectPtr(new Project(input, project_attributes));
  }

 protected:
  void getFieldStringItems(
      std::vector<std::string> *inline_field_names,
      std::vector<std::string> *inline_field_values,
      std::vector<std::string> *non_container_child_field_names,
      std::vector<OptimizerTreeBaseNodePtr> *non_container_child_fields,
      std::vector<std::string> *container_child_field_names,
      std::vector<std::vector<OptimizerTreeBaseNodePtr>> *container_child_fields) const override;

 private:
  Project(const PlanPtr &input,
          const std::vector<AttributeReferencePtr> &project_attributes)
      : input_(input),
        project_attributes_(project_attributes) {
    addChild(input_);
  }

  const PlanPtr input_;
  const std::vector<AttributeReferencePtr> project_attributes_;

  DISALLOW_COPY_AND_ASSIGN(Project);
};

}  // namespace optimizer
}  // namespace project

#endif  // PROJECT_OPTIMIZER_PROJECT_HPP_
