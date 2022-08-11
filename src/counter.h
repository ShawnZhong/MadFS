#pragma once

#include <chrono>
#include <magic_enum.hpp>
#include <mutex>

#include "debug.h"
#include "logging.h"

namespace ulayfs {

class Counter {
 private:
  std::array<size_t, magic_enum::enum_count<Event>()> occurrences{};
  std::array<size_t, magic_enum::enum_count<Event>()> sizes{};
  std::array<size_t, magic_enum::enum_count<Event>()> durations{};
  std::array<std::chrono::time_point<std::chrono::high_resolution_clock>,
             magic_enum::enum_count<Event>()>
      start_times;

 public:
  Counter() = default;
  ~Counter() { print(); }

  void count(Event event) { occurrences[magic_enum::enum_integer(event)]++; }
  void count(Event event, size_t size) {
    count(event);
    sizes[magic_enum::enum_integer(event)] += size;
  }

  void start_timer(Event event) {
    count(event);
    auto now = std::chrono::high_resolution_clock::now();
    start_times[magic_enum::enum_integer(event)] = now;
  }
  void start_timer(Event event, size_t size) {
    sizes[magic_enum::enum_integer(event)] += size;
    start_timer(event);
  }

  void end_timer(Event event) {
    auto end_time = std::chrono::high_resolution_clock::now();
    auto start_time = start_times[magic_enum::enum_integer(event)];
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end_time - start_time);
    durations[magic_enum::enum_integer(event)] += duration.count();
  }

  void clear() {
    occurrences.fill(0);
    sizes.fill(0);
    durations.fill(0);
  }

  [[nodiscard]] size_t get_occurrence(Event event) const {
    return occurrences[magic_enum::enum_integer(event)];
  }

 private:
  bool is_empty() {
    for (auto count : occurrences)
      if (count != 0) return false;
    return true;
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
      fprintf(log_file, "        %-25s: %zu",
              magic_enum::enum_name(event).data(), occurrence);

      // print duration
      if (size_t duration = durations[val]; duration != 0) {
        double avg_ns = (double)duration / (double)occurrence;
        fprintf(log_file, " (%.2f ms, %.2f us)", (double)duration / 1000000.0,
                avg_ns / 1000.0);
      }

      // print size
      if (size_t size = sizes[val]; size != 0) {
        double total_mb = (double)size / 1024.0 / 1024.0;
        double avg_kb = (double)size / 1024.0 / (double)occurrence;
        fprintf(log_file, " (%.2f MB, %.2f KB)", total_mb, avg_kb);
      }

      fprintf(log_file, "\n");
    });
  }
};

class DummyCounter {
 public:
  void count(Event) {}
  void count(Event, size_t) {}
  void start_timer(Event) {}
  void start_timer(Event, size_t) {}
  void end_timer(Event) {}
  void clear() {}
  size_t get_occurrence(Event) { return 0; }
};

static thread_local auto counter = []() {
  if constexpr (BuildOptions::enable_counter)
    return Counter{};
  else
    return DummyCounter{};
}();

}  // namespace ulayfs
