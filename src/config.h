#pragma once

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

#ifdef __CLWB__
constexpr static bool support_clwb = true;
#else
constexpr static bool support_clwb = false;
#endif

#ifdef __CLFLUSHOPT__
constexpr static bool support_clflushopt = true;
#else
constexpr static bool support_clflushopt = false;
#endif
};  // namespace BuildOptions

struct RuntimeOptions {
  RuntimeOptions(){
      // load options from environment variables
  };
};

}  // namespace ulayfs
