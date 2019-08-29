#ifndef PROJECT_STORAGE_COLUMN_VECTOR_HPP_
#define PROJECT_STORAGE_COLUMN_VECTOR_HPP_

#include <cstddef>
#include <cstdlib>

#include "storage/ColumnAccessor.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

namespace project {

class ColumnVector {
 public:
  ColumnVector() {}

  virtual ~ColumnVector() {}

  virtual std::size_t getTypeSize() const = 0;

  virtual std::size_t getNumTuples() const = 0;

  virtual ColumnVector* createSubset(const Range &range) const = 0;

  virtual ColumnAccessor* createColumnAccessor() const = 0;

  virtual std::uint64_t getValueVirtual(const std::size_t pos) const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(ColumnVector);
};

template <typename T>
class ColumnVectorImpl : public ColumnVector {
 public:
  typedef T ValueType;

  explicit ColumnVectorImpl(const std::size_t reserved_length)
      : reserved_length_(reserved_length),
        actual_length_(0),
        data_(static_cast<T*>(std::malloc(reserved_length * sizeof(T)))),
        owns_(true) {}

  ColumnVectorImpl(const std::size_t length, T *data, bool owns)
      : reserved_length_(length),
        actual_length_(length),
        data_(data),
        owns_(owns) {}

  ~ColumnVectorImpl() {
    if (owns_) {
      std::free(data_);
    }
  }

  std::size_t getTypeSize() const override {
    return sizeof(T);
  }

  std::size_t getNumTuples() const override {
    return actual_length_;
  }

  ColumnVector* createSubset(const Range &range) const override {
    return new ColumnVectorImpl<T>(range.size(),
                                   data_ + range.begin(),
                                   false /* owns */);
  }

  ColumnAccessor* createColumnAccessor() const override {
    return new ColumnStoreColumnAccessor<T>(actual_length_, data_);
  }

  std::uint64_t getValueVirtual(const std::size_t pos) const override {
    return at(pos);
  }

  inline void resize(const std::size_t length) {
    DCHECK_LE(length, reserved_length_);
    actual_length_ = length;
  }

  inline void appendValue(const T &value) {
    DCHECK_LT(actual_length_, reserved_length_);
    data_[actual_length_++] = value;
  }

  inline void setValue(const std::size_t pos, const T &value) {
    DCHECK_LT(pos, actual_length_);
    data_[pos] = value;
  }

  inline T& at(const std::size_t pos) const {
    DCHECK_LT(pos, actual_length_);
    return data_[pos];
  }

  inline T& operator[](const std::size_t pos) const {
    DCHECK_LT(pos, actual_length_);
    return data_[pos];
  }

 private:
  const std::size_t reserved_length_;
  std::size_t actual_length_;
  T *data_;
  bool owns_;

  DISALLOW_COPY_AND_ASSIGN(ColumnVectorImpl);
};

}  // namespace project

#endif  // PROJECT_STORAGE_COLUMN_VECTOR_HPP_

