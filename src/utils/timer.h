#pragma once

#include <chrono>
#include <magic_enum.hpp>
#include <mutex>

#include "const.h"
#include "timer_event.h"
#include "utils/logging.h"

namespace ulayfs {

class Timer {
 private:
  static constexpr auto NUM_EVENTS = magic_enum::enum_count<Event>();

  std::array<size_t, NUM_EVENTS> counts_{};
  std::array<size_t, NUM_EVENTS> sizes_{};
  std::array<std::chrono::nanoseconds, NUM_EVENTS> durations_{};
  std::array<std::chrono::high_resolution_clock::time_point, NUM_EVENTS> start_;

  template <Event event, typename T>
  constexpr auto&& get(std::array<T, NUM_EVENTS>& arr) {
    return std::get<magic_enum::enum_integer(event)>(arr);
  }

 public:
  Timer() = default;
  ~Timer() { print(); }

  template <Event event>
  void count() {
    if constexpr (!is_enabled<event>()) return;
    get<event>(counts_)++;
  }

  template <Event event>
  void count(size_t size) {
    if constexpr (!is_enabled<event>()) return;
    count<event>();
    get<event>(sizes_) += size;
  }

  template <Event event>
  void start() {
    if constexpr (!is_enabled<event>()) return;
    count<event>();
    get<event>(start_) = std::chrono::high_resolution_clock::now();
  }

  template <Event event>
  void start(size_t size) {
    if constexpr (!is_enabled<event>()) return;
    get<event>(sizes_) += size;
    start<event>();
  }

  template <Event event>
  void stop() {
    if constexpr (!is_enabled<event>()) return;
    auto end_time = std::chrono::high_resolution_clock::now();
    auto start_time = get<event>(start_);
    get<event>(durations_) += end_time - start_time;
  }

  void clear() {
    if constexpr (!BuildOptions::enable_timer) return;
    counts_.fill(0);
    sizes_.fill(0);
    durations_.fill({});
  }

  [[nodiscard]] size_t get_count(Event event) const {
    if constexpr (!BuildOptions::enable_timer) return 0;
    return counts_[magic_enum::enum_integer(event)];
  }

  [[nodiscard]] size_t get_size(Event event) const {
    if constexpr (!BuildOptions::enable_timer) return 0;
    return sizes_[magic_enum::enum_integer(event)];
  }

  [[nodiscard]] std::chrono::nanoseconds get_duration(Event event) const {
    if constexpr (!BuildOptions::enable_timer) return {};
    return durations_[magic_enum::enum_integer(event)];
  }

  void print() {
    static std::mutex print_mutex;

    if constexpr (!BuildOptions::enable_timer) return;
    if (is_empty()) return;
    std::lock_guard<std::mutex> guard(print_mutex);
    fprintf(log_file, "    [Thread %d] Timer:\n", tid);
    magic_enum::enum_for_each<Event>([&](auto val) {
      constexpr Event event = val;
      size_t count = get<event>(counts_);
      if (count == 0) return;

      // print name and count
      fprintf(log_file, "        %-25s: %7zu",
              magic_enum::enum_name(event).data(), count);

      // print duration
      if (auto duration = get<event>(durations_); duration.count() != 0) {
        double total_ms =
            std::chrono::duration<double, std::milli>(duration).count();
        double avg_us =
            std::chrono::duration<double, std::micro>(duration).count() /
            (double)count;

        fprintf(log_file, " (%8.3f us, %6.2f ms)", avg_us, total_ms);
      }

      // print size
      if (size_t size = get<event>(sizes_); size != 0) {
        double total_mb = (double)size / 1024.0 / 1024.0;
        double avg_kb = (double)size / 1024.0 / (double)count;
        fprintf(log_file, " (%6.2f KB, %6.2f MB)", avg_kb, total_mb);
      }

      fprintf(log_file, "\n");
    });
  }

  bool is_empty() {
    for (auto count : counts_)
      if (count != 0) return false;
    return true;
  }

  template <Event event>
  static constexpr bool is_enabled() {
    for (auto e : disabled_events) {
      if (e == event) return false;
    }

    return BuildOptions::enable_timer;
  }
};

inline thread_local Timer timer;

template <Event event>
struct TimerGuard {
  TimerGuard() {
    if constexpr (!BuildOptions::enable_timer) return;
    timer.start<event>();
  }
  explicit TimerGuard(size_t size) {
    if constexpr (!BuildOptions::enable_timer) return;
    timer.start<event>(size);
  }
  ~TimerGuard() {
    if constexpr (!BuildOptions::enable_timer) return;
    timer.stop<event>();
  }
};

}  // namespace ulayfs
