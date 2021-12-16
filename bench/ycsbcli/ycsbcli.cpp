#include <cassert>
#include <chrono>
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fstream>
#include <cmath>
#include <random>

#include "cxxopts.hpp"

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"


typedef enum {
    READ,
    INSERT,
    UPDATE,
    SCAN,
    UNKNOWN
} ycsb_op_t;

typedef struct {
    ycsb_op_t op;
    std::string key;
    size_t scan_len;    // only valid for scan
} ycsb_req_t;


static leveldb::ReadOptions read_options = leveldb::ReadOptions();
static leveldb::WriteOptions write_options = leveldb::WriteOptions();


/** Run/load YCSB workload on a leveldb instance. */
static uint
do_ycsb(std::string db_location, std::vector<ycsb_req_t>& reqs, std::string value)
{
    // Open a leveldb database.
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, db_location, &db);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        exit(1);
    }

    std::string read_buf;
    read_buf.reserve(value.length());
    uint cnt = 0;

    for (auto& req : reqs) {
        switch (req.op) {
            case INSERT:
            case UPDATE: {
                    status = db->Put(write_options, req.key, value);
                }
                break;

            case READ: {
                    status = db->Get(read_options, req.key, &read_buf);
                }
                break;

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
                }
                break;

            case UNKNOWN:
            default: {
                    std::cerr << "Error: unrecognized opcode" << std::endl;
                    exit(1);
                }
        }

        assert(status.ok());
        cnt++;
    }

    delete db;
    return cnt;
}


int
main(int argc, char *argv[])
{
    std::string db_location, ycsb_filename;
    uint value_size;

    cxxopts::Options cmd_args("leveldb ycsb trace exec client");
    cmd_args.add_options()
            ("h,help", "print help message", cxxopts::value<bool>()->default_value("false"))
            ("d,directory", "directory of db", cxxopts::value<std::string>(db_location)->default_value("./dbdir"))
            ("v,value_size", "size of value", cxxopts::value<uint>(value_size)->default_value("64"))
            ("f,ycsb", "YCSB trace filename", cxxopts::value<std::string>(ycsb_filename)->default_value(""))
            ("s,sync", "force write sync", cxxopts::value<bool>()->default_value("false"));
    auto result = cmd_args.parse(argc, argv);

    if (result.count("help")) {
        printf("%s", cmd_args.help().c_str());
        exit(0);
    }

    if (result.count("sync"))
        write_options.sync = true;

    // Read in YCSB workload trace.
    std::vector<ycsb_req_t> ycsb_reqs;
    if (!ycsb_filename.empty()) {
        std::ifstream input(ycsb_filename);
        std::string opcode;
        std::string key;
        while (input >> opcode >> key) {
            ycsb_op_t op = opcode == "READ"   ? READ
                         : opcode == "INSERT" ? INSERT
                         : opcode == "UPDATE" ? UPDATE
                         : opcode == "SCAN"   ? SCAN : UNKNOWN;
            size_t scan_len = 0;
            if (op == SCAN)
                input >> scan_len;
            ycsb_reqs.push_back(ycsb_req_t { .op=op, .key=key, .scan_len=scan_len });
        }
    } else {
        std::cerr << "Error: must give YCSB trace filename" << std::endl;
        printf("%s", cmd_args.help().c_str());
        exit(1);
    }

    if (ycsb_reqs.size() == 0) {
        std::cerr << "Error: given YCSB trace file has not valid lines" << std::endl;
        exit(1);
    }

    // Generate value.
    std::string value(value_size, '0');

    // Execute the actions of the YCSB trace.
    uint cnt = do_ycsb(db_location, ycsb_reqs, value);
    std::cout << "Finished " << cnt << " requests." << std::endl;

    return 0;
}
