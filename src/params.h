#pragma once

#include <cstdint>

namespace ulayfs {
// hardware parameters
constexpr static uint32_t BLOCK_SHIFT = 12;
constexpr static uint64_t BLOCK_SIZE = 1 << BLOCK_SHIFT;
constexpr static uint32_t CACHELINE_SHIFT = 6;
constexpr static uint32_t CACHELINE_SIZE = 1 << CACHELINE_SHIFT;

// grow in the unit of 2 MB
constexpr static uint32_t GROW_UNIT_SHIFT = 21;
constexpr static uint32_t GROW_UNIT_SIZE = 1 << GROW_UNIT_SHIFT;

// preallocate must be multiple of grow_unit
constexpr static uint32_t PREALLOC_SHIFT = 1 * GROW_UNIT_SHIFT;
constexpr static uint32_t PREALLOC_SIZE = 1 * GROW_UNIT_SIZE;

constexpr static uint32_t NUM_OFFSET_QUEUE_SLOT = 16;

}  // namespace ulayfs
