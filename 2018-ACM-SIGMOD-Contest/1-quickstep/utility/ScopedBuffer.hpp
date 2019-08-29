#ifndef PROJECT_UTILITY_SCOPED_BUFFER_HPP_
#define PROJECT_UTILITY_SCOPED_BUFFER_HPP_

#include <cstddef>
#include <cstdlib>
#include <cstring>

#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace project {

class ScopedBuffer {
 public:
  explicit ScopedBuffer(const std::size_t alloc_size,
                        const bool initialize = false) {
    internal_ptr_ = std::malloc(alloc_size);
    if (initialize) {
      std::memset(internal_ptr_, 0x0, alloc_size);
    }
  }

  explicit ScopedBuffer(void *memory = nullptr)
      : internal_ptr_(memory) {}

  ScopedBuffer(ScopedBuffer &&orig)
      : internal_ptr_(orig.internal_ptr_) {
    orig.internal_ptr_ = nullptr;
  }

  ~ScopedBuffer() {
    if (internal_ptr_ != nullptr) {
      std::free(internal_ptr_);
    }
  }

  inline ScopedBuffer& operator=(ScopedBuffer &&rhs) {
    if (internal_ptr_ != nullptr) {
      std::free(internal_ptr_);
    }
    internal_ptr_ = rhs.internal_ptr_;
    rhs.internal_ptr_ = nullptr;

    return *this;
  }

  inline void reset(const std::size_t alloc_size, const bool initialize = false) {
    if (internal_ptr_ != nullptr) {
      std::free(internal_ptr_);
    }
    internal_ptr_ = std::malloc(alloc_size);
    if (initialize) {
      memset(internal_ptr_, 0x0, alloc_size);
    }
  }

  inline void reset(void *memory = nullptr) {
    if (internal_ptr_ != NULL) {
      std::free(internal_ptr_);
    }
    internal_ptr_ = memory;
  }

  inline void resize(const std::size_t new_alloc_size) {
    DCHECK(internal_ptr_ != nullptr);
    if (new_alloc_size == 0) {
      reset();
    } else {
      internal_ptr_ = std::realloc(internal_ptr_, new_alloc_size);
    }
  }

  inline void* release() {
    void *memory = internal_ptr_;
    internal_ptr_ = nullptr;
    return memory;
  }

  inline bool empty() const {
    return internal_ptr_ == nullptr;
  }

  inline void* get() const {
    return internal_ptr_;
  }

 private:
  void *internal_ptr_;

  DISALLOW_COPY_AND_ASSIGN(ScopedBuffer);
};

}  // namespace project

#endif  // PROJECT_UTILITY_SCOPED_BUFFER_HPP_
