// struct.hpp
#pragma once
#include <vector>
#include <cstdint>

namespace templatedb {

class Value {
public:
    std::vector<int> items;
    bool visible = true;

    Value() {}
    Value(bool _visible) : visible(_visible) {}
    Value(std::vector<int> _items) : items(_items) {}

    bool operator==(const Value& other) const {
        return visible == other.visible && items == other.items;
    }
};

struct Entry {
    bool tomb = false;
    uint64_t seq;
    int key;
    Value val;
};

struct RangeTomb {
    int start, end;
    uint64_t seq;
};

struct Fragment {
    int start, end;
    uint64_t max_seq;
};

} // namespace templatedb
