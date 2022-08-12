#pragma once

#include <emmintrin.h>

#include <chrono>
#include <magic_enum.hpp>
#include <mutex>

#include "debug.h"
#include "logging.h"

namespace ulayfs {

class Timer {
 private:
  std::array<size_t, magic_enum::enum_count<Event>()> occurrences{};
  std::array<size_t, magic_enum::enum_count<Event>()> sizes{};
  std::array<std::chrono::nanoseconds, magic_enum::enum_count<Event>()>
      durations{};
  std::array<std::chrono::time_point<std::chrono::high_resolution_clock>,
             magic_enum::enum_count<Event>()>
      start_times;

 public:
  Timer() = default;
  ~Timer() { print(); }

  template <Event event>
  void count() {
    if constexpr (!BuildOptions::enable_timer) return;
    occurrences[magic_enum::enum_integer(event)]++;
  }

  template <Event event>
  void count(size_t size) {
    if constexpr (!BuildOptions::enable_timer) return;
    count<event>();
    sizes[magic_enum::enum_integer(event)] += size;
  }

  template <Event event>
  void start() {
    if constexpr (!BuildOptions::enable_timer) return;
    count<event>();
    auto now = std::chrono::high_resolution_clock::now();
    start_times[magic_enum::enum_integer(event)] = now;
  }

  template <Event event>
  void start(size_t size) {
    if constexpr (!BuildOptions::enable_timer) return;
    sizes[magic_enum::enum_integer(event)] += size;
    start<event>();
  }

  template <Event event>
  void stop() {
    if constexpr (!BuildOptions::enable_timer) return;
    auto end_time = std::chrono::high_resolution_clock::now();
    auto start_time = start_times[magic_enum::enum_integer(event)];
    durations[magic_enum::enum_integer(event)] += (end_time - start_time);
  }

  void clear() {
    if constexpr (!BuildOptions::enable_timer) return;
    occurrences.fill(0);
    sizes.fill(0);
    durations.fill({});
  }

  [[nodiscard]] size_t get_occurrence(Event event) const {
    if constexpr (!BuildOptions::enable_timer) return 0;
    return occurrences[magic_enum::enum_integer(event)];
  }

  void print() {
    static std::mutex print_mutex;

    if constexpr (!BuildOptions::enable_timer) return;
    if (is_empty()) return;
    std::lock_guard<std::mutex> guard(print_mutex);
    fprintf(log_file, "    [Thread %d] Timer:\n", tid);
    magic_enum::enum_for_each<Event>([&](Event event) {
      auto val = magic_enum::enum_integer(event);
      size_t occurrence = occurrences[val];
      if (occurrence == 0) return;

      // print name and occurrence
      fprintf(log_file, "        %-25s: %6zu",
              magic_enum::enum_name(event).data(), occurrence);

      // print duration
      if (auto duration = durations[val]; duration.count() != 0) {
        double total_ms =
            std::chrono::duration<double, std::milli>(duration).count();
        double avg_us =
            std::chrono::duration<double, std::micro>(duration).count() /
            (double)occurrence;

        fprintf(log_file, " (%6.3f us, %6.2f ms)", avg_us, total_ms);
      }

      // print size
      if (size_t size = sizes[val]; size != 0) {
        double total_mb = (double)size / 1024.0 / 1024.0;
        double avg_kb = (double)size / 1024.0 / (double)occurrence;
        fprintf(log_file, " (%6.2f KB, %6.2f MB)", avg_kb, total_mb);
      }

      fprintf(log_file, "\n");
    });
  }

 private:
  bool is_empty() {
    for (auto count : occurrences)
      if (count != 0) return false;
    return true;
  }
};

extern thread_local Timer timer;
}  // namespace ulayfs
