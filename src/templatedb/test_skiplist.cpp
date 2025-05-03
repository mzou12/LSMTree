#include <iostream>
#include <vector>
#include <algorithm>
#include "struct.hpp"
#include "SkipList.hpp"

using namespace templatedb;

std::ostream& operator<<(std::ostream& os, const Value& val) {
    if (!val.visible) {
        os << "[TOMBSTONE]";
    } else if (val.items.empty()) {
        os << "[EMPTY]";
    } else {
        os << "[";
        for (size_t i = 0; i < val.items.size(); ++i) {
            os << val.items[i];
            if (i + 1 != val.items.size()) os << ", ";
        }
        os << "]";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const templatedb::Entry& e) {
    os << "{key=" << e.key 
       << ", seq=" << e.seq 
       << ", tomb=" << (e.tomb ? "Y" : "N")
       << ", visible=" << (e.val.visible ? "Y" : "N")
       << ", items=";
    if (e.val.items.empty()) {
        os << "[]";
    } else {
        os << "[";
        for (size_t i = 0; i < e.val.items.size(); ++i) {
            os << e.val.items[i];
            if (i + 1 != e.val.items.size()) os << ",";
        }
        os << "]";
    }
    os << "}";
    return os;
}


int main() {
    // ================================
    // 1. 测试 SkipList<int, int>
    // ================================
    SkipList<int, int> list1;

    list1.insert(5, 50);
    list1.insert(2, 20);
    list1.insert(8, 80);
    list1.insert(5, 55); // 测试 key 冲突覆盖

    std::cout << "===== Test SkipList<int, int> =====" << std::endl;
    int idx = 0;
    for (auto it = list1.begin(); it != list1.end(); ++it, ++idx) {
        std::cout << "Index " << idx << ": Value = " << *it << std::endl;
    }

    std::cout << "list1[0] = " << list1[0] << std::endl;
    std::cout << "list1[1] = " << list1[1] << std::endl;

    std::cout << "Find key 5, value = " << list1.find(5).value() << std::endl;
    std::cout << "Contain key 2? " << (list1.contain(2) ? "Yes" : "No") << std::endl;
    std::cout << "Contain key 10? " << (list1.contain(10) ? "Yes" : "No") << std::endl;

    list1.print();
    list1.clear();
    std::cout << "After clear, list1 empty? " << (list1.empty() ? "Yes" : "No") << "\n\n";

    // ================================
    // 2. 测试 SkipList<int, Entry>
    // ================================
    SkipList<int, Entry> list2;

    list2.insert(3, Entry{false, 1, 3, Value({10, 20})});
    list2.insert(1, Entry{false, 2, 1, Value(std::vector<int>{30})});
    list2.insert(5, Entry{false, 3, 5, Value({40, 50, 60})});
    list2.insert(1, Entry{true, 4, 1, Value(false)});  // tombstone: key=1 被删除

    std::cout << "===== Test SkipList<int, Entry> =====" << std::endl;
    idx = 0;
    for (auto it = list2.begin(); it != list2.end(); ++it, ++idx) {
        const Entry& e = *it;
        std::cout << "Index " << idx
                  << ": key = " << e.key
                  << ", seq = " << e.seq
                  << ", tomb = " << (e.tomb ? "Yes" : "No")
                  << ", value = " << e.val
                  << std::endl;
    }

    Entry found = list2.find(1).value();
    std::cout << "Find key 1: tomb=" << (found.tomb ? "Yes" : "No")
              << ", value=" << found.val
              << ", seq=" << found.seq
              << std::endl;

    list2.print();
    list2.clear();
    std::cout << "After clear, list2 empty? " << (list2.empty() ? "Yes" : "No") << "\n";

    return 0;
}
