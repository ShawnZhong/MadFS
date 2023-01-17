#pragma once

#include <tbb/cache_aligned_allocator.h>

#include <cstring>

namespace madfs {
template <typename T>
class zero_allocator : public tbb::cache_aligned_allocator<T> {
 public:
  using value_type = T;
  using propagate_on_container_move_assignment = std::true_type;
  using is_always_equal = std::true_type;

  zero_allocator() = default;
  template <typename U>
  explicit zero_allocator(const zero_allocator<U> &) noexcept {};

  T *allocate(std::size_t n) {
    T *ptr = tbb::cache_aligned_allocator<T>::allocate(n);
    std::memset(static_cast<void *>(ptr), 0, n * sizeof(value_type));
    return ptr;
  }
};
}  // namespace madfs
