#include "MemTable.hpp"
#include "SSTable.hpp"
#include <cassert>
#include <iostream>

using namespace templatedb;

int main() {
    // 初始化 MemTable
    MemTable mem;
    mem.add(10, Value({1,3}), 1);
    mem.add(20, Value({2,3}), 2);
    mem.add(30, Value({3,3}), 3);
    mem.add(40, Value({4,4}), 4);
    mem.range_delete(42, 60, 5); // 覆盖 50，但 seq 更小
    mem.add(50, Value({5,3}), 6);

    // 插入有重叠的 range delete
    mem.range_delete(15, 35, 7); // 应该覆盖 20, 30
    mem.range_delete(25, 45, 8); // 覆盖 30, 40，并覆盖上面的 seq
    mem.add(70, Value({7,3}), 9);
    mem.point_delete(70, 10);
    mem.reset_iterator();
    Entry mem_test = mem.next().value();
    assert(mem_test.key == 10);
    assert(mem_test.seq == 1);
    assert(mem_test.val.items.at(0)==1);
    mem.next();
    mem.next();
    mem.next();
    mem_test = mem.next().value();
    assert(mem_test.key == 50);
    assert(mem_test.seq == 6);
    assert(mem_test.val.items.at(0)==5);
    assert(mem.has_next());
    mem_test = mem.next().value();
    assert(mem_test.key == 70);
    assert(mem_test.seq == 10);
    assert(mem_test.val.visible == false);
    mem_test = mem.next().value();
    assert(!mem.has_next());
    std::cout<< "delte val" << mem_test.seq<<"\n";
    assert(mem_test.seq == 9);
    mem.reset_range_iterator();
    RangeTomb mem_rb_test = mem.range_tombs_next().value();
    assert(mem_rb_test.start == 15);
    assert(mem_rb_test.end == 35);
    mem.range_tombs_next();
    std::cout<<"test last one" <<"\n";
    mem_rb_test = mem.range_tombs_next().value();
    assert(mem_rb_test.start == 42);
    assert(mem_rb_test.end == 60);
    assert(!mem.range_tombs_has_next());
    

    std::string path = "test_mem_save.txt";

    assert(mem.flush(path));

    // 加载 SSTable
    SSTable sst(path);

    // 测试 get
    assert(sst.get(10).has_value());            // 不在 range delete 内
    assert(sst.get(10).value().visible == true);

    assert(sst.get(20).value().visible == false); // 被第一个 tomb 覆盖
    assert(sst.get(30).value().visible == false); // 被 seq=7 覆盖
    assert(sst.get(40).value().visible == false); // 被 seq=7 覆盖
    assert(sst.get(50).value().visible == true);  // 虽然落入 tomb 范围，但 seq=5 小于 put 的 5，不成立
    assert(sst.get(50).value().items.at(0) == 5);
    assert(sst.get(50).value().items.at(1) == 3);
    assert(!sst.get(60).has_value());  

    // 检查 fragments
    std::cout << "Fragments:\n";
    for (const auto& frag : sst.getFragments()) {
        std::cout << "  [" << frag.start << ", " << frag.end << ") seq=" << frag.max_seq << "\n";
    }

    sst.reset_iterator();
    std::cout<<"start entry iterator test" <<"\n";
    Entry test = sst.next().value();
    assert(test.key == 10);
    assert(test.seq == 1);
    assert(test.val.items.at(0)==1);
    sst.next();
    sst.next();
    sst.next();
    test = sst.next().value();
    assert(test.key == 50);
    assert(test.seq == 6);
    assert(test.val.items.at(0)==5);

    assert(sst.hasRangeDelete());
    assert(sst.get_seq_start()==1);
    assert(sst.has_next());
    test = sst.next().value();
    assert(test.key == 70);
    assert(test.seq == 10);
    assert(test.val.visible == false);
    assert(!sst.has_next());
    std::cout<<"start range iterator test" <<"\n";
    sst.reset_range_iterator();
    RangeTomb rb_test = sst.range_tombs_next().value();
    assert(rb_test.start == 15);
    assert(rb_test.end == 35);
    sst.range_tombs_next();
    std::cout<<"test last one" <<"\n";
    rb_test = sst.range_tombs_next().value();
    assert(rb_test.start == 42);
    assert(rb_test.end == 60);
    assert(!sst.range_tombs_has_next());

    std::cout << "[PASS] All tests with overlapping range tombstones succeeded.\n";
    return 0;
}
