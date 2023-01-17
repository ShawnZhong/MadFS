#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>

#include "cxxopts.hpp"
#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

typedef enum { READ, INSERT, UPDATE, SCAN, UNKNOWN } ycsb_op_t;

typedef struct {
  ycsb_op_t op;
  std::string key;
  size_t scan_len;  // only valid for scan
} ycsb_req_t;

static leveldb::ReadOptions read_options;
static leveldb::WriteOptions write_options;

/** Run/load YCSB workload on a leveldb instance. */
static uint do_ycsb(const std::string& db_location,
                    const std::vector<ycsb_req_t>& reqs,
                    const std::string& value, double& microsecs) {
  // Open a leveldb database.
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 64 * 1024 * 1024;
  options.max_file_size = 64 * 1024 * 1024;
  leveldb::Status status = leveldb::DB::Open(options, db_location, &db);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    exit(1);
  }

  std::string read_buf;
  read_buf.reserve(value.length());
  uint cnt = 0;

  // Prepare for timing.
  auto time_start = std::chrono::high_resolution_clock::now();
  microsecs = 0;

  for (auto& req : reqs) {
    switch (req.op) {
      case INSERT:
      case UPDATE: {
        status = db->Put(write_options, req.key, value);
      } break;

      case READ: {
        status = db->Get(read_options, req.key, &read_buf);
      } break;

      case SCAN: {
        auto iter = db->NewIterator(read_options);
        iter->Seek(req.key);
        status = iter->status();
        for (size_t i = 0; i < req.scan_len; ++i) {
          if (iter->Valid()) {
            read_buf = iter->value().ToString();
            iter->Next();
          } else
            break;
        }
      } break;

      case UNKNOWN:
      default: {
        std::cerr << "Error: unrecognized opcode" << std::endl;
        exit(1);
      }
    }

    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
      exit(1);
    }

    cnt++;
  }

  // Calculate time elapsed.
  auto time_end = std::chrono::high_resolution_clock::now();
  microsecs =
      std::chrono::duration<double, std::milli>(time_end - time_start).count();

  delete db;
  return cnt;
}

int main(int argc, char* argv[]) {
  std::string db_location, ycsb_filename;
  uint value_size;

  cxxopts::Options cmd_args("leveldb ycsb trace exec client");
  auto add_opt = cmd_args.add_options();
  add_opt("h,help", "print help message",
          cxxopts::value<bool>()->default_value("false"));
  add_opt("d,directory", "directory of db",
          cxxopts::value<std::string>(db_location)->default_value("./dbdir"));
  add_opt("v,value_size", "size of value",
          cxxopts::value<uint>(value_size)->default_value("1000"));
  add_opt("f,ycsb", "YCSB trace filename",
          cxxopts::value<std::string>(ycsb_filename)->default_value(""));
  auto result = cmd_args.parse(argc, argv);

  if (result.count("help")) {
    printf("%s", cmd_args.help().c_str());
    exit(0);
  }

  // Read in YCSB workload trace.
  std::vector<ycsb_req_t> ycsb_reqs;
  if (!ycsb_filename.empty()) {
    std::ifstream input(ycsb_filename);
    std::string opcode;
    std::string table_name;
    std::string key;
    std::string rest;
    while (input >> opcode) {
      ycsb_op_t op = opcode == "READ"     ? READ
                     : opcode == "INSERT" ? INSERT
                     : opcode == "UPDATE" ? UPDATE
                     : opcode == "SCAN"   ? SCAN
                                          : UNKNOWN;
      if (op == UNKNOWN) continue;
      input >> table_name >> key;
      size_t scan_len = 0;
      if (op == SCAN) input >> scan_len;
      std::getline(input, rest);
      ycsb_reqs.push_back({.op = op, .key = key, .scan_len = scan_len});
    }
  } else {
    std::cerr << "Error: must give YCSB trace filename" << std::endl;
    printf("%s", cmd_args.help().c_str());
    exit(1);
  }

  if (ycsb_reqs.empty()) {
    std::cerr << "Error: given YCSB trace file has not valid lines"
              << std::endl;
    exit(1);
  }

  // Generate value.
  std::string value(value_size, '0');

  std::cout << "Running YCSB workload file \"" << ycsb_filename << "\" at \""
            << db_location << "\" with value size " << value_size << std::endl;

  // Execute the actions of the YCSB trace.
  double microsecs;
  uint cnt = do_ycsb(db_location, ycsb_reqs, value, microsecs);
  std::cout << "Finished " << cnt << " requests." << std::endl;
  if (microsecs > 0)
    std::cout << "Time elapsed: " << microsecs << " ms" << std::endl;

  return 0;
}
