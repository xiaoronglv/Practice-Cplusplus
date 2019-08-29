#ifndef PROJECT_STORAGE_DATABASE_HPP_
#define PROJECT_STORAGE_DATABASE_HPP_

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include "storage/Attribute.hpp"
#include "storage/Relation.hpp"
#include "storage/StorageTypedefs.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class Database {
 public:
  Database() {}

  std::size_t getNumRelations() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return relations_.size();
  }

  relation_id addRelation(Relation *relation) {
    std::lock_guard<std::mutex> lock(mutex_);
    const relation_id id = relations_.size();
    relation->setID(id);
    relations_.emplace_back(relation);
    return id;
  }

  const Relation& getRelation(const relation_id id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    DCHECK_LT(id, relations_.size());
    return *relations_[id];
  }

  Relation* getRelationMutable(const relation_id id) {
    std::lock_guard<std::mutex> lock(mutex_);
    DCHECK_LT(id, relations_.size());
    return relations_[id].get();
  }

  void dropRelation(const relation_id id) {
    std::unique_ptr<Relation> relation;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      relation = std::move(relations_[id]);
    }
  }

  bool isPrimaryKeyForeignKey(const Attribute *primary_key,
                              const Attribute *foreign_key) const {
    const auto it = pk_fk_index_.find(primary_key);
    if (it == pk_fk_index_.end()) {
      return false;
    }
    return it->second.find(foreign_key) != it->second.end();
  }

  void setPrimaryKeyForeignKey(const Attribute *primary_key,
                               const Attribute *foreign_key) {
    std::lock_guard<std::mutex> lock(mutex_);
    pk_fk_index_[primary_key].emplace(foreign_key);
    fk_index_.emplace(foreign_key);
  }

  template <typename Functor>
  void forEachPrimaryKey(const Functor &functor) const {
    for (const auto &it : pk_fk_index_) {
      functor(it.first);
    }
  }

  template <typename Functor>
  void forEachForeignKey(const Functor &functor) const {
    for (const Attribute *attribute : fk_index_) {
      functor(attribute);
    }
  }

  bool isForeignKey(const Attribute *attribute) const {
    return fk_index_.find(attribute) != fk_index_.end();
  }

  std::size_t getTotalNumTuples() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t total_num_tuples = 0;
    for (const auto &relation : relations_) {
      if (relation != nullptr) {
        total_num_tuples += relation->getNumTuples();
      }
    }
    return total_num_tuples;
  }

  std::string getBriefSummary() const {
    std::string summary;
    summary.append("Primary key foreign keys:\n");
    for (const auto &it : pk_fk_index_) {
      const Attribute &pk = *it.first;
      summary.append("[" + pk.getParentRelation().getName());
      summary.append("." + pk.getName() + "]");
      for (const auto &fk : it.second) {
        summary.append(" " + fk->getParentRelation().getName());
        summary.append("." + fk->getName());
      }
      summary.append("\n");
    }

    summary.append("\nPrimary key indexes:\n");
    forEachPrimaryKey([&](const Attribute *attr) {
      const auto &index_manager = attr->getParentRelation().getIndexManager();
      if (index_manager.hasPrimaryKeyIndex(attr->id())) {
        summary.append(attr->getParentRelation().getName());
        summary.append("." + attr->getName() + " ");
      }
    });

    summary.append("\n");
    summary.append("\nForeign key indexes:\n");
    forEachForeignKey([&](const Attribute *attr) {
      const auto &index_manager = attr->getParentRelation().getIndexManager();
      if (index_manager.hasForeignKeyIndex(attr->id())) {
        summary.append(attr->getParentRelation().getName());
        summary.append("." + attr->getName() + " ");
      }
    });
    summary.append("\n");

    return summary;
  }

 private:
  std::vector<std::unique_ptr<Relation>> relations_;

  struct AttributeComparator {
    inline bool operator()(const Attribute *lhs, const Attribute *rhs) const {
      const relation_id lhs_rid = lhs->getParentRelation().getID();
      const relation_id rhs_rid = rhs->getParentRelation().getID();
      if (lhs_rid != rhs_rid) {
        return lhs_rid < rhs_rid;
      }
      return lhs->id() < rhs->id();
    }
  };

  std::map<const Attribute*,
           std::set<const Attribute*, AttributeComparator>,
           AttributeComparator> pk_fk_index_;

  std::set<const Attribute*, AttributeComparator> fk_index_;

  mutable std::mutex mutex_;

  DISALLOW_COPY_AND_ASSIGN(Database);
};

}  // namespace project

#endif  // PROJECT_STORAGE_DATABASE_HPP_
