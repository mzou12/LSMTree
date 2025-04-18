#include "db.hpp"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace templatedb;

int main() {
    // 清理目录
    std::filesystem::remove_all("SSTables");
    std::filesystem::create_directory("SSTables");

    DB db;
    db.set_flush(5);             // flush 每 3 条
    db.set_level_size(10);        // compaction 每层 6 条
    db.set_level_size_multi(2);  // 下一层翻倍大小

    // 插入 10 条数据，会触发多轮 flush + compact
    std::cout << "start flush and compact"<<"\n";
    for (int i = 0; i < 13; ++i) {
        db.put(i, Value({i, i}));
    }
    std::cout<< "start delete"<<"\n";
    // point delete
    db.del(1);

    // range delete
    db.del(4, 6);

    for (int i = 13; i < 213; ++i) {
        db.put(i, Value({i, i}));
    }

    db.del(10, 12);
    std::cout<< "start get"<<"\n";
    // get
    assert(db.get(0).visible);
    assert(!db.get(1).visible);      // 被 point delete
    assert(!db.get(5).visible);      // 被 range delete
    assert(db.get(7).visible);
    assert(!db.get(10).visible);
    assert(!db.get(11).visible);
    db.put(5, Value({5,5}));
    assert(db.get(5).visible);
    assert(db.get(100).visible);
    assert(!db.get(1000).visible);
    db.del(100, 213);
    assert(!db.get(100).visible);

    // scan
    std::cout<< "start scan"<<"\n";
    auto vals = db.scan();
    std::cout << "[SCAN ALL] Returned " << vals.size() << " items\n";
    for (const auto& val : vals) {
        for (int v : val.items) std::cout << v << " ";
        std::cout << "\n";
    }

    // scan(min, max)
    std::cout<< "start scan for range"<<"\n";
    auto vals2 = db.scan(0, 5);
    std::cout << "[SCAN 0-5] Returned " << vals2.size() << " items\n";
    for (const auto& val : vals2) {
        for (int v : val.items) std::cout << v << " ";
        std::cout << "\n";
    }

    std::cout << "Pass\n";
    return 0;
}
