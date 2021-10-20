#pragma once

#include <cstdint>

namespace ulayfs {

namespace BuildOptions {
#ifdef MEM_PROTECT
constexpr static bool mem_protect = true;
#else
constexpr static bool mem_protect = false;
#endif

#ifdef RELAXED
constexpr static bool relaxed = true;
#else
constexpr static bool relaxed = false;
#endif

#ifdef NDEBUG
constexpr static bool debug = false;
#else
constexpr static bool debug = true;
#endif

#ifdef USE_HUGEPAGE
constexpr static bool use_hugepage = true;
#else
constexpr static bool use_hugepage = false;
#endif

constexpr static bool support_clwb = true;
constexpr static bool support_clflushopt = true;
};  // namespace BuildOptions

namespace LayoutOptions {
// grow in the unit of 2 MB
constexpr static uint32_t grow_unit_shift = 20;
constexpr static uint32_t grow_unit_size = 2 << grow_unit_shift;
// preallocate must be multiple of grow_unit
constexpr static uint32_t prealloc_shift = 1 * grow_unit_shift;
constexpr static uint32_t prealloc_size = 1 * grow_unit_size;
};  // namespace LayoutOptions

struct RuntimeOptions {
  RuntimeOptions(){
      // load options from environment variables
  };
};

}  // namespace ulayfs
