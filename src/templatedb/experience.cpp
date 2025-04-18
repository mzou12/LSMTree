#include "db.hpp"
#include <cassert>
#include <iostream>
#include <filesystem>
#include <chrono>

using namespace templatedb;

int main() {
    templatedb::DB db;
    db.set_flush(10);           
    db.set_level_size(20);     
    db.set_level_size_multi(2); 
    // 清理目录
    std::filesystem::remove_all("SSTables");
    std::filesystem::create_directory("SSTables");

    Operation op;
    std::cout << "hold1"<<"\n";
    std::vector<templatedb::Operation> ops = templatedb::Operation::ops_from_file("data/test_1000_2_200.wl");
    std::cout << "hold2"<<"\n";
    std::vector<templatedb::Value> result = db.execute_op(op);
    std::cout << "hold3"<<"\n";

    auto start = std::chrono::high_resolution_clock::now();
    for (const auto& op : ops) {
        db.execute_op(op);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Total execution time: " << duration.count() << " ms\n";

    return 0;

}