#ifndef PROJECT_UTILITY_PLAN_VISUALIZER_HPP_
#define PROJECT_UTILITY_PLAN_VISUALIZER_HPP_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "optimizer/Plan.hpp"
#include "optimizer/cost/SimpleCostModel.hpp"
#include "utility/Macros.hpp"

namespace project {

class PlanVisualizer {
 public:
  PlanVisualizer() : id_counter_(0) {}

  ~PlanVisualizer() {}

  std::string visualize(const optimizer::PlanPtr &input);

 private:
  struct NodeInfo {
    int id;
    std::vector<std::string> labels;
    std::string color;
  };

  struct EdgeInfo {
    int src_node_id;
    int dst_node_id;
    std::vector<std::string> labels;
    bool dashed;
  };

  void visit(const optimizer::PlanPtr &input);

  int id_counter_;
  std::unordered_map<optimizer::PlanPtr, int> node_id_map_;
  std::unordered_map<std::string, std::string> color_map_;

  std::vector<NodeInfo> nodes_;
  std::vector<EdgeInfo> edges_;

  const optimizer::SimpleCostModel cost_model_;

  DISALLOW_COPY_AND_ASSIGN(PlanVisualizer);
};

}  // namespace project

#endif  // PROJECT_UTILITY_PLAN_VISUALIZER_HPP_
