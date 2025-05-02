#include "db.hpp"
#include <cassert>
#include <iostream>
#include <filesystem>
#include <chrono>

using namespace templatedb;

int main() {
    templatedb::DB db;
    db.set_flush(100);           
    db.set_level_size(400);     
    db.set_level_size_multi(2); 
    // 清理目录
    std::filesystem::remove_all("SSTables");
    std::filesystem::create_directory("SSTables");

    Operation op;
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file("data/I_only_100000_3_100000.wl");
    // std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file("data/I_seq_100000_3_0.wl");
    std::vector<templatedb::Operation> ops_q = templatedb::Operation::ops_from_file("data/Q_only_10000_3_100000.wl");
    std::vector<templatedb::Value> result = db.execute_op(op);

    auto start = std::chrono::high_resolution_clock::now();
    for (const auto& op : ops) {
        db.execute_op(op);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total execution time: " << duration.count() << " ms\n";

    auto start_q = std::chrono::high_resolution_clock::now();
    for (const auto& op : ops_q) {
        db.execute_op(op);
    }
    auto end_q = std::chrono::high_resolution_clock::now();
    auto duration_q = std::chrono::duration_cast<std::chrono::milliseconds>(end_q - start_q);
    std::cout << "Total execution time query: " << duration_q.count() << " ms\n";

    return 0;

}