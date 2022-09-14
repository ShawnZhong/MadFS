#pragma once

#include <pthread.h>
#include <sys/xattr.h>

#include <atomic>
#include <ostream>

#include "const.h"
#include "idx.h"
#include "posix.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace ulayfs::dram {

class alignas(SHM_PER_THREAD_SIZE) PerThreadData {
  enum class State : uint8_t {
    UNINITIALIZED,
    PENDING,  // the state is inconsistent (initializing or destroying)
    INITIALIZED,
  };

  std::atomic<enum State> state;

  // mutex used to indicate the liveness of the thread
  // shall only be read when state == INITIALIZED
  pthread_mutex_t mutex;

  // the index within the shared memory region
  size_t index;

  // each thread will pin a tx block so that the garbage collector will not
  // reclaim this block and blocks after it
  std::atomic<LogicalBlockIdx> tx_block_idx;

 public:
  /**
   * @return true if there are some data stored, regardless of whether the
   * thread is alive or not
   */
  [[nodiscard]] bool has_data() const { return state != State::UNINITIALIZED; }

  /**
   * @return true if this PerThreadData contains valid data (i.e. the state
   * is initialized and the thread is not dead).
   */
  [[nodiscard]] bool is_data_valid() {
    if (state != State::INITIALIZED) return false;
    return is_thread_alive();
  }

  /**
   * Try to initialize the per-thread data. There should be only one thread
   * calling this function at a time.
   *
   * @param i the index of this per-thread data
   * @return true if initialization succeeded
   */
  bool try_init(size_t i) {
    State expected = State::UNINITIALIZED;
    if (!state.compare_exchange_strong(expected, State::PENDING,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
      // If the state is not UNINITIALIZED, then it must be INITIALIZED.
      // It cannot be INITIALIZING because there should be only one thread
      // calling this function.
      return false;
    }

    index = i;
    tx_block_idx.store(0, std::memory_order_relaxed);
    init_robust_mutex(&mutex);
    // TODO: uncomment this
    //    pthread_mutex_lock(&mutex);
    state.store(State::INITIALIZED, std::memory_order_release);

    LOG_DEBUG("PerThreadData %ld initialized by tid %d", i, tid);
    return true;
  }

  /**
   * Destroy the per-thread data
   */
  void reset() {
    State expected = State::INITIALIZED;
    if (!state.compare_exchange_strong(expected, State::PENDING,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
      // if the old state is already PENDING or UNINITIALIZED, then we
      // don't need to do anything
      return;
    }
    LOG_DEBUG("PerThreadData %ld to be reset by tid %d", index, tid);
    // TODO: uncomment this
    //    if (is_thread_alive()) pthread_mutex_unlock(&mutex);
    index = 0;
    tx_block_idx.store(0, std::memory_order_relaxed);
    pthread_mutex_destroy(&mutex);
    state.store(State::UNINITIALIZED, std::memory_order_release);
  }

  void set_tx_block_idx(LogicalBlockIdx idx) {
    tx_block_idx.store(idx, std::memory_order_relaxed);
  }

  [[nodiscard]] LogicalBlockIdx get_tx_block_idx() const {
    return tx_block_idx.load(std::memory_order_relaxed);
  }

 private:
  /**
   * Check the robust mutex to see if the thread is alive.
   *
   * This function can only be called when state == INITIALIZED, since trying to
   * lock an uninitialized mutex will cause undefined behavior.
   *
   * @return true if the thread is alive
   */
  bool is_thread_alive() {
    return true;  // TODO: fix me
    assert(state.load(std::memory_order_acquire) == State::INITIALIZED);
    int rc = pthread_mutex_trylock(&mutex);
    if (rc == 0) {
      // if we can lock the mutex, then the thread is dead
      pthread_mutex_unlock(&mutex);
      return false;
    } else if (rc == EBUSY) {
      // if the mutex is already locked, then the thread is alive
      return true;
    } else if (rc == EOWNERDEAD) {
      // detected that the owner of the mutex is dead
      pthread_mutex_consistent(&mutex);
      pthread_mutex_unlock(&mutex);
      return false;
    } else {
      PANIC("pthread_mutex_trylock failed: %s", strerror(rc));
    }
  }

 public:
  friend std::ostream& operator<<(std::ostream& os, PerThreadData& data) {
    std::array state_names = {"UNINITIALIZED", "PENDING", "INITIALIZED"};
    State curr_state = data.state;

    os << "PerThreadData{state="
       << state_names[static_cast<size_t>(curr_state)];
    if (curr_state == State::INITIALIZED) {
      os << ", is_thread_alive=" << data.is_thread_alive();
    }
    os << ", tx_block_idx=" << data.tx_block_idx << "}";
    return os;
  }
};

static_assert(sizeof(PerThreadData) == SHM_PER_THREAD_SIZE);

class ShmMgr {
  pmem::MetaBlock* meta;
  int fd = -1;
  void* addr = nullptr;
  char path[SHM_PATH_LEN]{};

 public:
  /**
   * Open and memory map the shared memory. If the shared memory does not exist,
   * create it.
   *
   * @param file_fd the file descriptor of the file that uses this shared memory
   * @param stat the stat of the file that uses this shared memory
   */
  ShmMgr(int file_fd, const struct stat& stat, pmem::MetaBlock* meta)
      : meta(meta) {
    // get or set the path of the shared memory
    {
      ssize_t rc = fgetxattr(file_fd, SHM_XATTR_NAME, path, SHM_PATH_LEN);
      if (rc == -1 && errno == ENODATA) {  // no shm_path attribute, create one
        sprintf(path, "/dev/shm/ulayfs_%016lx_%013lx", stat.st_ino,
                (stat.st_ctim.tv_sec * 1000000000 + stat.st_ctim.tv_nsec) >> 3);
        rc = fsetxattr(file_fd, SHM_XATTR_NAME, path, SHM_PATH_LEN, 0);
        PANIC_IF(rc == -1, "failed to set shm_path attribute");
      } else if (rc == -1) {
        PANIC("failed to get shm_path attribute");
      }
    }

    // use posix::open instead of shm_open since shm_open calls open, which is
    // overloaded by ulayfs
    fd = posix::open(path, O_RDWR | O_NOFOLLOW | O_CLOEXEC, S_IRUSR | S_IWUSR);
    if (fd < 0) {
      fd = create(path, stat.st_mode, stat.st_uid, stat.st_gid);
    }
    LOG_DEBUG("posix::open(%s) = %d", path, fd);

    addr = posix::mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
                       fd, 0);
    if (addr == MAP_FAILED) {
      posix::close(fd);
      PANIC("mmap shared memory failed");
    }
  }

  ~ShmMgr() {
    if (fd >= 0) posix::close(fd);
    if (addr != nullptr) posix::munmap(addr, SHM_SIZE);
  }

  [[nodiscard]] void* get_bitmap_addr() const { return addr; }

  /**
   * Get the address of the per-thread data of the current thread.
   * Shall only be called by the garbage collector.
   *
   * @param idx the index of the per-thread data
   * @return the address of the per-thread data
   */
  [[nodiscard]] PerThreadData* get_per_thread_data(size_t idx) const {
    assert(idx < MAX_NUM_THREADS);
    char* starting_addr = static_cast<char*>(addr) + TOTAL_NUM_BITMAP_BYTES;
    return reinterpret_cast<PerThreadData*>(starting_addr) + idx;
  }

  /**
   * Allocate a new per-thread data for the current thread.
   * @return the address of the per-thread data
   */
  [[nodiscard]] PerThreadData* alloc_per_thread_data() const {
    // TODO: make sure that only one thread can allocate a per-thread data at a
    //  time. Otherwise, a thread crashes during PerThreadData::try_init will
    //  result in a leak (e.g., state == PENDING but the thread is dead).
    // meta->lock();
    for (size_t i = 0; i < MAX_NUM_THREADS; i++) {
      PerThreadData* per_thread_data = get_per_thread_data(i);
      bool success = per_thread_data->try_init(i);
      if (success) return per_thread_data;
    }
    // meta->unlock();
    PANIC("No empty per-thread data");
  }

  /**
   * Remove the shared memory object associated.
   */
  void unlink() const { unlink_by_shm_path(path); }

  /**
   * Create a shared memory object.
   *
   * @param shm_path the path of the shared memory object
   * @param mode the mode of the shared memory object
   * @param uid the uid of the shared memory object
   * @param gid the gid of the shared memory object
   * @return the file descriptor of the shared memory object
   */
  static int create(const char* shm_path, mode_t mode, uid_t uid, gid_t gid) {
    // We create a temporary file first, and then use `linkat` to put the file
    // into the directory `/dev/shm`. This ensures the atomicity of the creating
    // the shared memory file and setting its permission.
    int shm_fd =
        posix::open("/dev/shm", O_TMPFILE | O_RDWR | O_NOFOLLOW | O_CLOEXEC,
                    S_IRUSR | S_IWUSR);
    if (unlikely(shm_fd < 0)) {
      PANIC("create the temporary file failed");
    }

    // change permission and ownership of the new shared memory
    if (fchmod(shm_fd, mode) < 0) {
      posix::close(shm_fd);
      PANIC("fchmod on shared memory failed");
    }

    if (fchown(shm_fd, uid, gid) < 0) {
      posix::close(shm_fd);
      PANIC("fchown on shared memory failed");
    }

    if (posix::fallocate(shm_fd, 0, 0, static_cast<off_t>(SHM_SIZE)) < 0) {
      posix::close(shm_fd);
      PANIC("fallocate on shared memory failed");
    }

    // publish the created tmpfile.
    char tmpfile_path[PATH_MAX];
    sprintf(tmpfile_path, "/proc/self/fd/%d", shm_fd);
    int rc =
        linkat(AT_FDCWD, tmpfile_path, AT_FDCWD, shm_path, AT_SYMLINK_FOLLOW);
    if (rc < 0) {
      // Another process may have created a new shared memory before us. Retry
      // opening.
      posix::close(shm_fd);
      shm_fd = posix::open(shm_path, O_RDWR | O_NOFOLLOW | O_CLOEXEC,
                           S_IRUSR | S_IWUSR);
      if (shm_fd < 0) {
        PANIC("cannot open or create the shared memory object %s", shm_path);
      }
    }

    return shm_fd;
  }

  /**
   * Remove the shared memory object given its path.
   * @param shm_path the path of the shared memory object
   */
  static void unlink_by_shm_path(const char* shm_path) {
    int ret = posix::unlink(shm_path);
    LOG_TRACE("posix::unlink(%s) = %d", shm_path, ret);
    if (unlikely(ret < 0))
      LOG_WARN("Could not unlink shm file \"%s\": %m", shm_path);
  }

  /**
   * Remove the shared memory object given the path of the file that uses it.
   * @param filepath the path of the file that uses the shared memory object
   */
  static void unlink_by_file_path(const char* filepath) {
    char shm_path[SHM_PATH_LEN];
    if (getxattr(filepath, SHM_XATTR_NAME, shm_path, SHM_PATH_LEN) <= 0) return;
    unlink_by_shm_path(shm_path);
  }

  friend std::ostream& operator<<(std::ostream& os, const ShmMgr& mgr) {
    __msan_scoped_disable_interceptor_checks();
    os << "ShmMgr:\n"
       << "\tfd = " << mgr.fd << "\n"
       << "\taddr = " << mgr.addr << "\n"
       << "\tpath = " << mgr.path << "\n";
    for (size_t i = 0; i < MAX_NUM_THREADS; ++i) {
      PerThreadData* per_thread_data = mgr.get_per_thread_data(i);
      if (!per_thread_data->has_data()) continue;
      os << "\t" << i << ": " << *mgr.get_per_thread_data(i) << "\n";
    }
    __msan_scoped_enable_interceptor_checks();
    return os;
  }
};

}  // namespace ulayfs::dram
