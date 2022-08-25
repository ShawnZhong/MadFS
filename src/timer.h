#pragma once

#include <chrono>
#include <magic_enum.hpp>
#include <mutex>

#include "const.h"
#include "logging.h"

namespace ulayfs {

class Timer {
 private:
  static constexpr auto NUM_EVENTS = magic_enum::enum_count<Event>();

  std::array<size_t, NUM_EVENTS> occurrences{};
  std::array<size_t, NUM_EVENTS> sizes{};
  std::array<std::chrono::nanoseconds, NUM_EVENTS> durations{};
  std::array<std::chrono::high_resolution_clock::time_point, NUM_EVENTS>
      start_times;

  template <Event event, typename T>
  constexpr auto&& get(std::array<T, NUM_EVENTS>& arr) {
    return std::get<magic_enum::enum_integer(event)>(arr);
  }

 public:
  Timer() = default;
  ~Timer() { print(); }

  template <Event event>
  void count() {
    if constexpr (!BuildOptions::enable_timer) return;
    get<event>(occurrences)++;
  }

  template <Event event>
  void count(size_t size) {
    if constexpr (!BuildOptions::enable_timer) return;
    count<event>();
    get<event>(sizes) += size;
  }

  template <Event event>
  void start() {
    if constexpr (!BuildOptions::enable_timer) return;
    count<event>();
    get<event>(start_times) = std::chrono::high_resolution_clock::now();
  }

  template <Event event>
  void start(size_t size) {
    if constexpr (!BuildOptions::enable_timer) return;
    get<event>(sizes) += size;
    start<event>();
  }

  template <Event event>
  void stop() {
    if constexpr (!BuildOptions::enable_timer) return;
    auto end_time = std::chrono::high_resolution_clock::now();
    auto start_time = get<event>(start_times);
    get<event>(durations) += end_time - start_time;
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
    magic_enum::enum_for_each<Event>([&](auto val) {
      constexpr Event event = val;
      size_t occurrence = get<event>(occurrences);
      if (occurrence == 0) return;

      // print name and occurrence
      fprintf(log_file, "        %-25s: %6zu",
              magic_enum::enum_name(event).data(), occurrence);

      // print duration
      if (auto duration = get<event>(durations); duration.count() != 0) {
        double total_ms =
            std::chrono::duration<double, std::milli>(duration).count();
        double avg_us =
            std::chrono::duration<double, std::micro>(duration).count() /
            (double)occurrence;

        fprintf(log_file, " (%6.3f us, %6.2f ms)", avg_us, total_ms);
      }

      // print size
      if (size_t size = get<event>(sizes); size != 0) {
        double total_mb = (double)size / 1024.0 / 1024.0;
        double avg_kb = (double)size / 1024.0 / (double)occurrence;
        fprintf(log_file, " (%6.2f KB, %6.2f MB)", avg_kb, total_mb);
      }

      fprintf(log_file, "\n");
    });
  }

  bool is_empty() {
    for (auto count : occurrences)
      if (count != 0) return false;
    return true;
  }
};

inline thread_local Timer timer;

template <Event event>
struct TimerGuard {
  TimerGuard() { timer.start<event>(); }
  explicit TimerGuard(size_t size) { timer.start<event>(size); }
  ~TimerGuard() { timer.stop<event>(); }
};
}  // namespace ulayfs
