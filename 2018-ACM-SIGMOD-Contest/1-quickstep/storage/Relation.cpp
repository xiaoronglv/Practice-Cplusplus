#include "storage/Relation.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "storage/Attribute.hpp"
#include "storage/AttributeStatistics.hpp"
#include "storage/IndexManager.hpp"
#include "types/Type.hpp"

#include "glog/logging.h"

namespace project {

Relation::Relation(const std::vector<const Type*> &attribute_types,
                   const bool is_temporary)
    : is_temporary_(is_temporary),
      id_(-1),
      statistics_(attribute_types.size()),
      index_manager_(attribute_types.size()) {
  for (std::size_t i = 0; i < attribute_types.size(); ++i) {
    DCHECK(attribute_types[i] != nullptr);
    attributes_.emplace_back(
        std::make_unique<Attribute>(i, *attribute_types[i], this));
  }
}

std::string Relation::getBriefSummary() const {
  std::string summary;
  summary.append("# columns = " + std::to_string(getNumAttributes()));
  summary.append(", # blocks = " + std::to_string(blocks_.size()));
  summary.append(", # tuples = " + std::to_string(statistics_.getNumTuples()));
  summary.append("\n");

  for (std::size_t i = 0; i < getNumAttributes(); ++i) {
    summary.append("  [attr " + std::to_string(i) + "]");
    summary.append(" type = " + attributes_[i]->getType().getName());
    const AttributeStatistics &stat = getStatistics().getAttributeStatistics(i);
    if (stat.hasMinValue()) {
      summary.append(", min = " + std::to_string(stat.getMinValue()));
    }
    if (stat.hasMaxValue()) {
      summary.append(", max = " + std::to_string(stat.getMaxValue()));
    }
    if (stat.hasNumDistinctValues()) {
      summary.append(", # distinct = " + std::to_string(stat.getNumDistinctValues()));
    }
    if (stat.getSortOrder() == SortOrder::kAscendant ||
        stat.getSortOrder() == SortOrder::kDescendant) {
      summary.append(", sort order = " + stat.getSortOrderString());
    }

    summary.append("\n");
  }
  return summary;
}

}  // namespace project
