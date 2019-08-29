#ifndef PROJECT_UTILITY_NUMERIC_GENERATOR_HPP_
#define PROJECT_UTILITY_NUMERIC_GENERATOR_HPP_

namespace project {

template <typename T, T step>
class NumericGenerator {
 public:
  class ConstIterator {
   public:
    ConstIterator() {}

    inline explicit ConstIterator(const std::size_t value)
        : value_(value) {}

    inline std::size_t operator*() const {
      return value_;
    }

    inline ConstIterator& operator++() {
      value_ += step;
      return *this;
    }

    inline ConstIterator& operator--() {
      value_ -= step;
      return *this;
    }

    inline ConstIterator operator++(int) {
      ConstIterator result(*this);
      ++(*this);
      return result;
    }

    inline ConstIterator operator--(int) {
      ConstIterator result(*this);
      --(*this);
      return result;
    }

    inline bool operator==(const ConstIterator& other) const {
      return value_ == other.value_;
    }

    inline bool operator!=(const ConstIterator& other) const {
      return !(*this == other);
    }

   private:
    T value_;
  };

  typedef ConstIterator const_iterator;

  inline NumericGenerator(const T begin, const T end)
      : begin_(begin), end_(end) {}

  inline const_iterator begin() const {
    return const_iterator(begin_);
  }

  inline const_iterator end() const {
    return const_iterator(end_);
  }

 private:
  const T begin_;
  const T end_;
};

}  // namespace project

#endif  // PROJECT_UTILITY_NUMERIC_GENERATOR_HPP_
