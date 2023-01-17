#include <cstdarg>

#include "lib.h"
#include "shm.h"
#include "utils/timer.h"

namespace madfs {

extern "C" {
int unlink(const char* path) {
  dram::ShmMgr::unlink_by_file_path(path);
  int rc = posix::unlink(path);
  LOG_DEBUG("posix::unlink(%s) = %d", path, rc);
  return rc;
}

int rename(const char* oldpath, const char* newpath) {
  if (access(newpath, F_OK) == 0) dram::ShmMgr::unlink_by_file_path(newpath);
  int rc = posix::rename(oldpath, newpath);
  LOG_DEBUG("posix::rename(%s, %s) = %d", oldpath, newpath, rc);
  return rc;
}

int truncate([[maybe_unused]] const char* path, [[maybe_unused]] off_t length) {
  PANIC("truncate not implemented");
  return -1;
}

int ftruncate([[maybe_unused]] int fd, [[maybe_unused]] off_t length) {
  PANIC("ftruncate not implemented");
  return -1;
}

int flock([[maybe_unused]] int fd, [[maybe_unused]] int operation) {
  PANIC("flock not implemented");
  return -1;
}

int fcntl(int fd, int cmd, ... /* arg */) {
  return 0;
  va_list arg;
  va_start(arg, cmd);
  auto res = posix::fcntl(fd, cmd, arg);
  va_end(arg);
  LOG_DEBUG("posix::fcntl(%d, %d, ...) = %d", fd, cmd, res);
  return res;
}

int fcntl64(int fd, int cmd, ... /* arg */) {
  return 0;
  va_list arg;
  va_start(arg, cmd);
  auto res = posix::fcntl(fd, cmd, arg);
  va_end(arg);
  LOG_DEBUG("posix::fcntl(%d, %d, ...) = %d", fd, cmd, res);
  return res;
}
}
}  // namespace madfs
