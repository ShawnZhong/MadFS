#include "persist.h"

#if ULAYFS_USE_LIBPMEM2
#include <libpmem2/persist.h>
pmem2_memcpy_fn pmem2_memcpy = []() {
  pmem2_persist_init();
  struct pmem2_map map;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  map.effective_granularity = PMEM2_GRANULARITY_CACHE_LINE;
  pmem2_set_mem_fns(&map);
  return map.memcpy_fn;
}();
#endif
