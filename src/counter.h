#pragma once

#include <emmintrin.h>

#include <chrono>
#include <magic_enum.hpp>
#include <mutex>

#include "debug.h"
#include "logging.h"

namespace ulayfs {

namespace detail {

class Counter {
 private:
  std::array<size_t, magic_enum::enum_count<Event>()> occurrences{};
  std::array<size_t, magic_enum::enum_count<Event>()> sizes{};
  std::array<std::chrono::nanoseconds, magic_enum::enum_count<Event>()>
      durations{};
  std::array<std::chrono::time_point<std::chrono::high_resolution_clock>,
             magic_enum::enum_count<Event>()>
      start_times;

 public:
  Counter() = default;
  ~Counter() { print(); }

  template <Event event>
  void count() {
    occurrences[magic_enum::enum_integer(event)]++;
  }

  template <Event event>
  void count(size_t size) {
    count<event>();
    sizes[magic_enum::enum_integer(event)] += size;
  }

  template <Event event>
  void start_timer() {
    count<event>();
    auto now = std::chrono::high_resolution_clock::now();
    start_times[magic_enum::enum_integer(event)] = now;
  }

  template <Event event>
  void start_timer(size_t size) {
    sizes[magic_enum::enum_integer(event)] += size;
    start_timer<event>();
  }

  template <Event event>
  void stop_timer(bool fence = false) {
    if (fence) _mm_mfence();
    auto end_time = std::chrono::high_resolution_clock::now();
    auto start_time = start_times[magic_enum::enum_integer(event)];
    durations[magic_enum::enum_integer(event)] += (end_time - start_time);
  }

  void clear() {
    occurrences.fill(0);
    sizes.fill(0);
    durations.fill({});
  }

  [[nodiscard]] size_t get_occurrence(Event event) const {
    return occurrences[magic_enum::enum_integer(event)];
  }

  void print() {
    static std::mutex print_mutex;

    if (is_empty()) return;
    std::lock_guard<std::mutex> guard(print_mutex);
    fprintf(log_file, "    [Thread %d] Counters:\n", tid);
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

class DummyCounter {
 public:
  template <Event event>
  void count() {}

  template <Event event>
  void count(size_t) {}

  template <Event event>
  void start_timer() {}

  template <Event event>
  void start_timer(size_t) {}

  template <Event event>
  void stop_timer() {}

  void clear() {}

  size_t get_occurrence(Event) { return 0; }

  void print() {}
};

static auto make_counter() {
  if constexpr (BuildOptions::enable_counter)
    return Counter{};
  else
    return DummyCounter{};
}

}  // namespace detail

using Counter = decltype(detail::make_counter());

extern thread_local Counter counter;
}  // namespace ulayfs
