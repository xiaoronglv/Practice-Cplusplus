#ifndef PROJECT_UTILITY_THREAD_SAFE_POOL_HPP_
#define PROJECT_UTILITY_THREAD_SAFE_POOL_HPP_

#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "utility/Macros.hpp"

template <typename T, typename Allocator>
class ThreadSafePool {
 public:
  explicit ThreadSafePool(const Allocator &allocator)
      : allocator_(allocator) {
    objects_.reserve(std::thread::hardware_concurrency());
  }

  T* allocate() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!objects_.empty()) {
        auto *object = objects_.back().release();
        objects_.pop_back();
        return object;
      }
    }
    return allocator_();
  }

  void push_back(T *object) {
    std::lock_guard<std::mutex> lock(mutex_);
    objects_.emplace_back(object);
  }

  template <typename Functor>
  void forEach(const Functor &functor) const {
    for (const auto &object : objects_) {
      functor(*object);
    }
  }

 private:
  const Allocator allocator_;
  std::vector<std::unique_ptr<T>> objects_;
  std::mutex mutex_;

  DISALLOW_COPY_AND_ASSIGN(ThreadSafePool);
};

#endif  // PROJECT_UTILITY_THREAD_SAFE_POOL_HPP_
