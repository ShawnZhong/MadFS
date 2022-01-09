#include <stdlib.h>
#include <linux/limits.h>

int error(sqlite3 *sqlite, sqlite3_stmt *sqlite_stmt);

static const char* get_db_path() {
  char* pmem_path = getenv("PMEM_PATH");
  if (!pmem_path) return "tpcc.db";
  static char path[PATH_MAX];
  strcpy(path, pmem_path);
  strcat(path, "/tpcc.db");
  return path;
};

#define  TIMESTAMP_LEN  80
#define  STRFTIME_FORMAT	"%Y-%m-%d %H:%M:%S"

