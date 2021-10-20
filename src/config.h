#pragma once

#include <ostream>
namespace ulayfs {

constexpr static struct BuildOptions {
  constexpr static const char* git_branch = GIT_BRANCH;
  constexpr static const char* git_commit_hash = GIT_COMMIT_HASH;

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

#ifdef NO_SHOW_CONFIG
  constexpr static bool show_config = false;
#else
  constexpr static bool show_config = true;
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

  friend std::ostream& operator<<(std::ostream& out, const BuildOptions& _) {
    out << "BuildOptions: \n";
    out << "\tgit_branch: " << git_branch << "\n";
    out << "\tgit_commit_hash: " << git_commit_hash << "\n";
    out << "\tmem_protect: " << mem_protect << "\n";
    out << "\trelaxed: " << relaxed << "\n";
    out << "\tdebug: " << debug << "\n";
    out << "\tuse_hugepage: " << use_hugepage << "\n";
    out << "\tsupport_clwb: " << support_clwb << "\n";
    out << "\tsupport_clflushopt: " << support_clflushopt << "\n";
    return out;
  }
} build_options;

struct RuntimeOptions {
  RuntimeOptions(){
      // load options from environment variables
  };
};

}  // namespace ulayfs
