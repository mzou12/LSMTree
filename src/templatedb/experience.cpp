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
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file("data/test_10000_3_10000.wl");
    std::vector<templatedb::Value> result = db.execute_op(op);

    auto start = std::chrono::high_resolution_clock::now();
    for (const auto& op : ops) {
        db.execute_op(op);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total execution time: " << duration.count() << " ms\n";

    return 0;

}