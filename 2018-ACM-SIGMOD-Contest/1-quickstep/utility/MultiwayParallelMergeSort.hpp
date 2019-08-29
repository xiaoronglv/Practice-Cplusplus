#ifndef PROJECT_UTILITY_MULTIWAY_PARALLEL_MERGE_SORT_HPP_
#define PROJECT_UTILITY_MULTIWAY_PARALLEL_MERGE_SORT_HPP_

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <queue>
#include <vector>

#include "scheduler/Task.hpp"
#include "utility/BitManipulation.hpp"
#include "utility/Macros.hpp"
#include "utility/Range.hpp"

#include "glog/logging.h"

namespace project {

namespace internal {

template <typename T>
struct ArrayReference {
  inline ArrayReference() {}
  inline ArrayReference(const T *data_in, const std::size_t length_in)
      : data(data_in), length(length_in) {}
  inline void reset(const T *data_in, const std::size_t length_in) {
    data = data_in;
    length = length_in;
  }
  inline const T* at(const std::size_t pos) const {
    return pos < length ? data + pos : nullptr;
  }
  const T *data;
  std::size_t length;
};

template <typename T>
struct ValueSource {
  inline ValueSource() {}
  inline ValueSource(const T *value_in, const std::size_t source_in)
      : value(value_in), source(source_in) {}
  inline void reset(const T *value_in, const std::size_t source_in) {
    value = value_in;
    source = source_in;
  }
  const T *value;
  std::size_t source;
};

template <typename T, typename ValueComparator>
struct ValueSourceComparator {
  explicit ValueSourceComparator(const ValueComparator &value_comp_in)
      : value_comp(value_comp_in) {}

  inline bool operator()(const ValueSource<T> &lhs,
                         const ValueSource<T> &rhs) const {
    const T *lv = lhs.value;
    const T *rv = rhs.value;

    // NULL represents infinity.
    const bool lv_is_null = (lv == nullptr);
    const bool rv_is_null = (rv == nullptr);

    if (lv_is_null || rv_is_null) {
      if (lv_is_null != rv_is_null) {
        return rv_is_null;
      }
    } else {
      if (value_comp(*lv, *rv)) {
        return true;
      }
      if (value_comp(*rv, *lv)) {
        return false;
      }
    }
    return lhs.source < rhs.source;
  }

  const ValueComparator &value_comp;
};

template <typename T, typename ValueComparator>
struct ValueSourceReverseComparator {
  explicit ValueSourceReverseComparator(const ValueComparator &value_comp_in)
      : value_comp(value_comp_in) {}

  inline bool operator()(const ValueSource<T> &lhs,
                         const ValueSource<T> &rhs) const {
    const T *lv = lhs.value;
    const T *rv = rhs.value;

    // NULL represents infinity.
    const bool lv_is_null = (lv == nullptr);
    const bool rv_is_null = (rv == nullptr);

    if (lv_is_null || rv_is_null) {
      if (lv_is_null != rv_is_null) {
        return lv_is_null;
      }
    } else {
      if (value_comp(*rv, *lv)) {
        return true;
      }
      if (value_comp(*lv, *rv)) {
        return false;
      }
    }
    return rhs.source < lhs.source;
  }

  const ValueComparator &value_comp;
};

template <typename T, typename ValueComparator>
struct ValuePointerComparator {
  explicit ValuePointerComparator(const ValueComparator &value_comp_in)
      : value_comp(value_comp_in) {}

  inline bool operator()(const T *lv, const T *rv) const {
    // NULL represents infinity.
    const bool lv_is_null = (lv == nullptr);
    const bool rv_is_null = (rv == nullptr);

    if (lv_is_null || rv_is_null) {
      return !lv_is_null;
    }

    return value_comp(*lv, *rv);
  }

  const ValueComparator &value_comp;
};

template <typename T, typename Comparator>
inline std::vector<std::size_t> MultiSeqPartition(
    const std::vector<ArrayReference<T>> &seqs,
    const std::size_t rank,
    const Comparator &comp) {
  const std::size_t m = seqs.size();

  std::size_t sum_len = 0;
  std::size_t max_len = 0;
  for (std::size_t i = 0; i < m; ++i) {
    const std::size_t length = seqs[i].length;
    sum_len += length;
    max_len = std::max(max_len, length);
  }

  DCHECK_GT(sum_len, 0);
  DCHECK_LE(rank, sum_len);

  const std::size_t range = (1ull << (MostSignificantBit(max_len) + 1)) - 1;
  const double f = static_cast<double>(rank) / (range * m);

  ValueSourceComparator<T, Comparator> l_comp(comp);
  ValueSourceReverseComparator<T, Comparator> r_comp(comp);
  ValuePointerComparator<T, Comparator> lp_comp(comp);

  std::vector<ValueSource<T>> samples;
  samples.reserve(m);

  std::size_t step = range / 2;

  for (std::size_t i = 0; i < m; ++i) {
    samples.emplace_back(seqs[i].at(step), i);
  }

  std::sort(samples.begin(), samples.end(), l_comp);

  std::vector<std::size_t> l_bounds(m);
  std::size_t l_size = static_cast<std::size_t>(m * f);
  std::size_t r_size = m - l_size;

  for (std::size_t i = 0; i < l_size; ++i) {
    l_bounds[samples[i].source] = step + 1;
  }
  for (std::size_t i = l_size; i < m; ++i) {
    l_bounds[samples[i].source] = 0;
  }

  const T *r_min = (l_size == m ? nullptr : samples[l_size].value);

  while (step > 0) {
    step /= 2;
    l_size *= 2;
    r_size *= 2;

    for (std::size_t i = 0; i < m; ++i) {
      const std::size_t mid = l_bounds[i] + step;
      const T *value = seqs[i].at(mid);
      if (lp_comp(value, r_min)) {
        l_bounds[i] = mid + 1;
        ++l_size;
      } else {
        ++r_size;
      }
    }

    const std::size_t l_expected_size =
        step == 0 ? rank : static_cast<std::size_t>((l_size + r_size) * f);

    if (l_size > l_expected_size) {
      std::priority_queue<ValueSource<T>,
                          std::vector<ValueSource<T>>,
                          decltype(l_comp)> l_local_max(l_comp);
      for (std::size_t i = 0; i < m; ++i) {
        const std::size_t lb = l_bounds[i];
        if (lb > 0) {
          l_local_max.emplace(seqs[i].at(lb - 1), i);
        }
      }

      const std::size_t skew = l_size - l_expected_size;
      for (std::size_t i = 0; i < skew; ++i) {
        DCHECK(!l_local_max.empty());
        const std::size_t idx = l_local_max.top().source;
        l_local_max.pop();

        DCHECK_GE(l_bounds[idx], step + 1);
        const std::size_t pos = l_bounds[idx] - 1;
        r_min = seqs[idx].at(pos);

        const std::size_t new_lb = pos - step;
        if (new_lb > 0) {
          l_local_max.emplace(seqs[idx].at(new_lb - 1), idx);
        }
        l_bounds[idx] = new_lb;
      }
      l_size -= skew;
      r_size += skew;
    } else if (l_size < l_expected_size) {
      std::priority_queue<ValueSource<T>,
                          std::vector<ValueSource<T>>,
                          decltype(r_comp)> r_local_min(r_comp);
      for (std::size_t i = 0; i < m; ++i) {
        const std::size_t rb = l_bounds[i] + step;
        if (rb < range) {
          r_local_min.emplace(seqs[i].at(rb), i);
        }
      }

      const std::size_t skew = l_expected_size - l_size;
      for (std::size_t i = 0; i < skew; ++i) {
        DCHECK(!r_local_min.empty());
        const std::size_t idx = r_local_min.top().source;
        r_local_min.pop();

        const std::size_t rb = (l_bounds[idx] += step + 1) + step;
        if (rb < range) {
          r_local_min.emplace(seqs[idx].at(rb), idx);
        }
      }
      if (r_local_min.empty()) {
        r_min = nullptr;
      } else {
        r_min = r_local_min.top().value;
      }
      l_size += skew;
      r_size -= skew;
    }
  }

  return l_bounds;
}

template <typename T, typename Comparator, bool handle_one_child>
void MultiSeqMergeInternal(std::vector<const T*> &heap,  // NOLINT[runtime/references]
                           const std::size_t num_values,
                           T *output,
                           const Comparator &comp) {
  const std::size_t num_nodes = heap.size();
  DCHECK_GE(num_nodes, 1ul);

  const std::size_t num_non_leaf_nodes_with_two_children = (num_nodes - 1) / 2;
  for (std::size_t i = 0; i < num_values; ++i) {
    const T *value = heap[0];
    output[i] = *value;
    ++value;

    std::size_t k = 0;
    while (k < num_non_leaf_nodes_with_two_children) {
      const std::size_t l = k * 2 + 1;
      const std::size_t r = l + 1;
      const T *lv = heap[l];
      const T *rv = heap[r];

      if (comp(*lv, *rv)) {
        if (comp(*lv, *value)) {
          heap[k] = lv;
          k = l;
        } else {
          break;
        }
      } else {
        if (comp(*rv, *value)) {
          heap[k] = rv;
          k = r;
        } else {
          break;
        }
      }
    }

    if (handle_one_child && k == num_non_leaf_nodes_with_two_children) {
      const std::size_t l = k * 2 + 1;
      const T *lv = heap[l];

      if (comp(*lv, *value)) {
        heap[k] = lv;
        k = l;
      }
    }

    heap[k] = value;
  }
}

template <typename T, typename Comparator>
void MultiSeqMerge(const std::vector<const T*> seq_begins,
                   const std::size_t num_values,
                   T *output,
                   const Comparator &comp) {
  DCHECK_GE(seq_begins.size(), 1ul);

  std::vector<const T*> heap(seq_begins);
  std::sort(heap.begin(), heap.end(),
            ValuePointerComparator<T, Comparator>(comp));

  if (heap.size() % 2 == 0) {
    MultiSeqMergeInternal<T, Comparator, true>(heap, num_values, output, comp);
  } else {
    MultiSeqMergeInternal<T, Comparator, false>(heap, num_values, output, comp);
  }
}

template <typename T, typename Comparator>
class MultiwayMergeSorter {
 public:
  MultiwayMergeSorter(T *data,
                      const std::size_t length,
                      const Comparator &comp,
                      const T &guardian)
      : data_(data),
        length_(length),
        comp_(comp),
        guardian_(guardian) {}

  inline void operator()(Task *ctx) {
    if (length_ == 0) {
      return;
    }

    const std::size_t num_partitions = length_ / kBatchSize + 1;

    if (num_partitions == 1) {
      std::sort(data_, data_ + length_, comp_);
      return;
    }

    auto sort_local_task = [this, num_partitions](Task *internal) {
      this->sequences_.resize(num_partitions);
      this->refs_.resize(num_partitions);
      this->num_tails_.store(0, std::memory_order_relaxed);

      const RangeSplitter splitter =
          RangeSplitter::CreateWithNumPartitions(0, length_, num_partitions);

      for (std::size_t i = 0; i < num_partitions; ++i) {
        const Range range = splitter[i];
        this->sequences_[i].reset(range.size() + 1 /* guardian */);

        internal->spawnLambdaTask([this, i, range] {
          const std::size_t local_length = range.size();
          T *local_data = this->sequences_[i].get();

          std::memcpy(local_data,
                      this->data_ + range.begin(),
                      local_length * sizeof(T));

          std::sort(local_data,
                    local_data + local_length,
                    this->comp_);

          local_data[local_length] = guardian_;

          std::size_t trunc_pos = local_length - 1;
          for (; trunc_pos != std::numeric_limits<std::size_t>::max();
               --trunc_pos) {
            if (comp_(local_data[trunc_pos], guardian_)) {
              break;
            }
          }

          const std::size_t trunc_length = trunc_pos + 1;
          this->refs_[i].reset(local_data, trunc_length);
          this->num_tails_.fetch_add(local_length - trunc_length,
                                     std::memory_order_relaxed);
        });
      }
    };

    auto split_task = [this, num_partitions](Task *ctx) {
      this->splits_.resize(num_partitions);
      this->ranks_.reserve(num_partitions);

      // Rebalance
      const std::size_t actual_length =
          this->length_ - this->num_tails_.load(std::memory_order_relaxed);
      const RangeSplitter splitter =
          RangeSplitter::CreateWithNumPartitions(0, actual_length, num_partitions);

      std::size_t rank = 0;
      for (std::size_t i = 0; i < num_partitions; ++i) {
        rank += splitter[i].size();

        ctx->spawnLambdaTask([i, rank, this]{
          this->splits_[i] = MultiSeqPartition(this->refs_, rank, this->comp_);
        });

        this->ranks_.emplace_back(rank);
      }
    };

    auto merge_task = [this, num_partitions](Task *ctx) {
      for (std::size_t i = 0; i < num_partitions; ++i) {
        ctx->spawnLambdaTask([i, num_partitions, this]{
          const std::size_t offset = (i == 0 ? 0 : this->ranks_[i-1]);
          const std::size_t num_values = this->ranks_[i] - offset;

          std::vector<const T*> seq_begins;
          seq_begins.reserve(num_partitions);
          for (std::size_t j = 0; j < num_partitions; ++j) {
            seq_begins.emplace_back(
                this->refs_[j].data + (i == 0 ? 0 : this->splits_[i-1][j]));
          }

          MultiSeqMerge(seq_begins, num_values, this->data_ + offset, this->comp_);
        });
      }
    };

    auto tail_task = [num_partitions, this](Task *ctx) {
      const std::size_t num_tails =
          this->num_tails_.load(std::memory_order_relaxed);

      // TODO(jianqiao): Handle the case.
      CHECK_EQ(0u, num_tails);
    };

    // Now construct the DAG.
    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask(sort_local_task),
        CreateLambdaTask(split_task),
        CreateLambdaTask(merge_task),
        CreateLambdaTask(tail_task)));
  }

 private:
  T *data_;
  const std::size_t length_;
  const Comparator comp_;
  const T guardian_;

  std::vector<ScopedArray<T>> sequences_;
  std::vector<ArrayReference<T>> refs_;
  std::vector<std::size_t> ranks_;
  std::vector<std::vector<std::size_t>> splits_;

  std::atomic<std::size_t> num_tails_;

  static constexpr std::size_t kBatchSize = 0x40000;

  DISALLOW_COPY_AND_ASSIGN(MultiwayMergeSorter);
};

}  // namespace internal

template <typename T, typename Comparator, typename Callback>
inline void MultiwayMergeSort(Task *ctx,
                              T *data,
                              const std::size_t length,
                              const T &guardian,
                              const Comparator &comparator,
                              const Callback &callback) {
  using SorterType = internal::MultiwayMergeSorter<T, Comparator>;

  std::shared_ptr<SorterType> sorter =
      std::make_shared<SorterType>(data, length, comparator, guardian);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([sorter](Task *internal) {
        (*sorter)(internal);
      }),
      CreateLambdaTask([sorter, callback](Task *internal) {
        callback(internal);
      })));
}

template <typename T, typename Comparator>
inline void MultiwayMergeSort(Task *ctx,
                              T *data,
                              const std::size_t length,
                              const T &guardian,
                              const Comparator &comparator) {
  using SorterType = internal::MultiwayMergeSorter<T, Comparator>;

  std::shared_ptr<SorterType> sorter =
      std::make_shared<SorterType>(data, length, comparator, guardian);

  ctx->spawnTask(CreateTaskChain(
      CreateLambdaTask([sorter](Task *internal) {
        (*sorter)(internal);
      }),
      CreateLambdaTask([sorter, length, data] {})));
}

}  // namespace project

#endif  // PROJECT_UTILITY_MULTIWAY_PARALLEL_MERGE_SORT_HPP_
