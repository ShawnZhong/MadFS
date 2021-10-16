#pragma once

#include <cstdint>
#ifdef MEM_PROTECT
#define MEM_PROTECT true
#else
#define MEM_PROTECT false
#endif

#ifdef RELAXED
#define RELAXED true
#else
#define RELAXED false
#endif

#ifdef NDEBUG
#define DEBUG false
#else
#define DEBUG true
#endif

namespace ulayfs {

namespace BuildOptions {
constexpr static bool mem_protect = MEM_PROTECT;
constexpr static bool relaxed = RELAXED;
constexpr static bool debug = DEBUG;
};  // namespace BuildOptions

namespace LayoutOptions {
// preallocate 2 MB
constexpr static uint32_t prealloc_size = (2 << 20);
};  // namespace LayoutOptions

struct RuntimeOptions {
  RuntimeOptions(){
      // load options from environment variables
  };
};

}  // namespace ulayfs
