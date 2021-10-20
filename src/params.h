#pragma once

namespace ulayfs {
// hardware parameters
constexpr static uint32_t BLOCK_SHIFT = 12;
constexpr static uint32_t BLOCK_SIZE = 1 << BLOCK_SHIFT;
constexpr static uint32_t CACHELINE_SHIFT = 6;
constexpr static uint32_t CACHELINE_SIZE = 1 << CACHELINE_SHIFT;

// parameters related to the file layout
namespace LayoutParams {
// grow in the unit of 2 MB
constexpr static uint32_t grow_unit_shift = 20;
constexpr static uint32_t grow_unit_size = 2 << grow_unit_shift;

// preallocate must be multiple of grow_unit
constexpr static uint32_t prealloc_shift = 1 * grow_unit_shift;
constexpr static uint32_t prealloc_size = 1 * grow_unit_size;
};  // namespace LayoutParams

}  // namespace ulayfs
