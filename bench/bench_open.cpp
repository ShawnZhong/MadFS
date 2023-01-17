#include <sys/stat.h>

#include <cxxopts.hpp>
#include <filesystem>

#include "common.h"
#include "posix.h"

void prep(const std::filesystem::path& file_path, uint64_t file_size) {
  unlink(file_path.c_str());

  int fd = open(file_path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    throw std::runtime_error("open failed");
  }

  prefill_file(fd, file_size, 4096);

  struct stat st {};
  ulayfs::posix::fstat(fd, &st);
  printf("file size: %.3f MB\n", st.st_size / 1024. / 1024.);

  close(fd);
}

void bench_open(const std::filesystem::path& file_path) {
  int fd = open(file_path.c_str(), O_RDONLY);
  assert(fd >= 0);
  close(fd);
}

struct Args {
  bool prep = false;
  bool open = false;
  uint64_t file_size = 4096;
  std::filesystem::path file_path = "test.txt";

  static Args parse(int argc, char** argv) {
    cxxopts::Options options("bench_open", "Open benchmark");
    Args args;
    options.add_options(
        {},
        {
            {"f,file", "Path to the file to store data",
             cxxopts::value<std::filesystem::path>(args.file_path)},
            {"p,prepare", "Prepare the file", cxxopts::value<bool>(args.prep)},
            {"o,open", "Open the file", cxxopts::value<bool>(args.open)},
            {"s,size", "File size in bytes",
             cxxopts::value<uint64_t>(args.file_size)},
            {"help", "Print help"},
        });

    auto result = options.parse(argc, argv);
    if (result.count("help")) {
      std::cout << options.help() << std::endl;
      exit(0);
    }
    return args;
  }
};

int main(int argc, char** argv) {
  const auto args = Args::parse(argc, argv);
  if (args.prep) prep(args.file_path, args.file_size);
  if (args.open) bench_open(args.file_path);
}
