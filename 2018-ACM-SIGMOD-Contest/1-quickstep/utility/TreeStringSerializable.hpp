#ifndef PROJECT_TREE_STRING_SERIALIZABLE_HPP_
#define PROJECT_TREE_STRING_SERIALIZABLE_HPP_

#include <cstddef>
#include <string>
#include <vector>

#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

template<class TreeNodeType>
class TreeStringSerializable {
 public:
  static const int kMaxLineWidth = 80;

  virtual ~TreeStringSerializable() {}

  virtual std::string getName() const = 0;

  std::string toString() const {
    return getNodeString(true /* is_root */,
                         true /* is_last */,
                         "" /* parent_prefix */,
                         "" /* node_name */);
  }

  std::string getShortString() const {
    std::vector<std::string> inline_field_names;
    std::vector<std::string> inline_field_values;
    std::vector<std::string> non_container_child_field_names;
    std::vector<TreeNodeType> non_container_child_fields;
    std::vector<std::string> container_child_field_names;
    std::vector<std::vector<TreeNodeType>> container_child_fields;

    getFieldStringItems(&inline_field_names,
                        &inline_field_values,
                        &non_container_child_field_names,
                        &non_container_child_fields,
                        &container_child_field_names,
                        &container_child_fields);
    return getHeadString(true /* is_root */,
                         true /* is_last */,
                         "" /* prefix */,
                         "" /* node_name */,
                         inline_field_names,
                         inline_field_values,
                         false /* multi_line */);
  }

 protected:
  TreeStringSerializable() {}

  virtual void getFieldStringItems(std::vector<std::string> *inline_field_names,
                                   std::vector<std::string> *inline_field_values,
                                   std::vector<std::string> *non_container_child_field_names,
                                   std::vector<TreeNodeType> *non_container_child_fields,
                                   std::vector<std::string> *container_child_field_names,
                                   std::vector<std::vector<TreeNodeType>> *container_child_fields) const = 0;

  std::string getNodeString(bool is_root,
                            bool is_last,
                            const std::string &parent_prefix,
                            const std::string &node_name) const {
    std::vector<std::string> inline_field_names;
    std::vector<std::string> inline_field_values;
    std::vector<std::string> non_container_child_field_names;
    std::vector<TreeNodeType> non_container_child_fields;
    std::vector<std::string> container_child_field_names;
    std::vector<std::vector<TreeNodeType>> container_child_fields;

    getFieldStringItems(&inline_field_names,
                        &inline_field_values,
                        &non_container_child_field_names,
                        &non_container_child_fields,
                        &container_child_field_names,
                        &container_child_fields);
    DCHECK_EQ(non_container_child_field_names.size(), non_container_child_fields.size());
    DCHECK_EQ(container_child_field_names.size(), container_child_field_names.size());
    DCHECK(!is_root || parent_prefix.empty());
    DCHECK(!is_root || is_last);

    std::string ret = getHeadString(is_root, is_last, parent_prefix, node_name, inline_field_names, inline_field_values,
                                    true /* multi_line */);
    ret.append("\n");

    std::string child_prefix;
    if (!is_root) {
      child_prefix = GetNewIndentedStringPrefix(is_last, parent_prefix);
    }

    // Append strings of non_container child fields.
    for (int i = 0; i < static_cast<int>(non_container_child_fields.size()) - 1; ++i) {
      ret.append(
          non_container_child_fields[i]->getNodeString(false /* is_root */,
                                                       false /* is_last */,
                                                       child_prefix,
                                                       non_container_child_field_names[i]));
    }

    if (!non_container_child_fields.empty()) {
      if (container_child_fields.empty()) {
        ret.append(
            non_container_child_fields.back()->getNodeString(false /* is_root */,
                                                             true /* is_last */,
                                                             child_prefix,
                                                             non_container_child_field_names.back()));
      } else {
        ret.append(
            non_container_child_fields.back()->getNodeString(false /* is_root */,
                                                             false /* is_last */,
                                                             child_prefix,
                                                             non_container_child_field_names.back()));
      }
    }

    // Append strings of container child fields.
    for (int i = 0; i < static_cast<int>(container_child_fields.size()) - 1; ++i) {
      ret.append(GetNodeListString(false /* is_last */,
                                   child_prefix,
                                   container_child_field_names[i],
                                   container_child_fields[i]));
    }

    if (!container_child_fields.empty()) {
      ret.append(
          GetNodeListString(true /* is_last */,
                            child_prefix,
                            container_child_field_names.back(),
                            container_child_fields.back()));
    }

    return ret;
  }

  static std::string GetNodeListString(bool is_last,
                                       const std::string &prefix,
                                       const std::string &node_name,
                                       const std::vector<TreeNodeType> &node_list) {
    std::string ret;
    std::string item_prefix = prefix;
    if (!node_name.empty()) {
      ret = item_prefix;
      ret.append("+-").append(node_name).append("=").append("\n");
      // Indent the child fields only if the node name is not empty.
      item_prefix = GetNewIndentedStringPrefix(is_last, prefix);
    }

    if (node_list.empty()) {
      ret.append(item_prefix).append("+-[]\n");
      return ret;
    }

    for (int i = 0; i < static_cast<int>(node_list.size()) - 1; ++i) {
      // Do not provide the node name so that there will not be an extra indent.
      ret.append(node_list[i]->getNodeString(false /* is_root */,
                                             false /* is_last */,
                                             item_prefix,
                                             "" /* node_name */));
    }
    if (!node_name.empty()) {
      ret.append(node_list.back()->getNodeString(false /* is_root */,
                                                 true /* is_last */,
                                                 item_prefix,
                                                 "" /* node_name */));
    } else {
      ret.append(node_list.back()->getNodeString(false /* is_root */,
                                                 (node_name.empty() ? is_last : true),
                                                 item_prefix, "" /* node_name */));
    }
    return ret;
  }

  std::string getHeadString(bool is_root,
                            bool is_last,
                            const std::string &parent_prefix,
                            const std::string &node_name,
                            const std::vector<std::string> &field_names,
                            const std::vector<std::string> &field_values,
                            bool multi_line) const {
    DCHECK_EQ(field_names.size(), field_values.size());
    std::string ret;

    const std::string name = getName();
    std::string prefix = GetNewIndentedStringPrefix(is_last, parent_prefix);
    std::string current_line = parent_prefix;
    if (!is_root) {
      current_line.append("+-");
    }

    if (!node_name.empty()) {
      current_line.append(node_name).append("=");
      if (current_line.size() + name.size() > kMaxLineWidth && multi_line) {
        ret.append(current_line).append("\n");
        current_line = prefix;
      }
    }

    current_line.append(name);

    if (!field_names.empty()) {
      // "[" and "]" are considered as inline fields
      // so that we can add newlines for them.
      AppendInlineString(prefix, "[" /* inline_field */, multi_line, &current_line, &ret);
      for (size_t i = 0; i < field_names.size(); ++i) {
        std::string inline_field = field_names[i];
        inline_field.append("=").append(field_values[i]);
        if (i < field_names.size() - 1) {
          inline_field.append(",");
        }
        AppendInlineString(prefix, inline_field, multi_line, &current_line, &ret);
      }
      AppendInlineString(prefix, "]" /* inline_field */, false /* multi_line */, &current_line, &ret);
    }

    if (!current_line.empty()) {
      ret.append(current_line);
    }

    return ret;
  }

  static void AppendInlineString(const std::string &prefix,
                                 const std::string &inline_field,
                                 bool multi_line,
                                 std::string *current_line,
                                 std::string *output) {
    if (current_line->size() + inline_field.size() > kMaxLineWidth && multi_line) {
      output->append(*current_line).append("\n");
      *current_line = prefix;
    }
    current_line->append(inline_field);
  }

  static std::string GetNewIndentedStringPrefix(bool is_last, const std::string &prefix) {
    std::string new_prefix(prefix);
    if (!is_last) {
      new_prefix.append("|").append(" ");
    } else {
      new_prefix.append(2, ' ');
    }
    return new_prefix;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(TreeStringSerializable);
};

}  // namespace project

#endif  // PROJECT_TREE_STRING_SERIALIZABLE_HPP_

