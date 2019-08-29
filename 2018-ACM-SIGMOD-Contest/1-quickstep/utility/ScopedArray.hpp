#ifndef PROJECT_UTILITY_SCOPED_ARRAY_HPP_
#define PROJECT_UTILITY_SCOPED_ARRAY_HPP_

#include <cstddef>
#include <utility>

#include "utility/Macros.hpp"
#include "utility/ScopedBuffer.hpp"

namespace project {

template <typename T>
class ScopedArray {
 public:
  explicit ScopedArray(const std::size_t length, const bool initialize = false)
      : buffer_(length * sizeof(T), initialize) {}

  explicit ScopedArray(T *data = nullptr)
      : buffer_(data) {}

  explicit ScopedArray(ScopedArray &&orig)
      : buffer_(std::move(orig.buffer_)) {}

  inline void reset(const std::size_t length, const bool initialize = false) {
    buffer_.reset(length * sizeof(T), initialize);
  }

  inline void reset(T *data = nullptr) {
    buffer_.reset(data);
  }

  inline ScopedArray& operator=(ScopedArray &&rhs) {
    buffer_ = std::move(rhs.buffer_);
    return *this;
  }

  inline T* get() const {
    return static_cast<T*>(buffer_.get());
  }

  inline T* release() {
    return static_cast<T*>(buffer_.release());
  }

  inline T* operator->() const {
    return get();
  }

  inline T& operator[](const std::size_t index) const {
    return get()[index];
  }

 private:
  ScopedBuffer buffer_;
};

}  // namespace project

#endif  // PROJECT_UTILITY_SCOPED_ARRAY_HPP_
