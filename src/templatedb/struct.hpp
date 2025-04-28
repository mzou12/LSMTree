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
    bool operator<(const Entry& other) const {
        if (key != other.key)
            return key < other.key;
        return seq > other.seq;
    }
};

struct RangeTomb {
    int start, end;
    uint64_t seq;
    bool operator<(const RangeTomb& other) const{
        if (start != other.start){
            return start < other.start;
        }
        return seq > other.seq;
    }
};

struct Fragment {
    int start, end;
    uint64_t max_seq;
    bool operator<(const Fragment& other) const {
        if (start ==  other.start){
            return start < other.start;
        }
        return max_seq > other.max_seq;
    }
};

template<typename T>
struct Node{
    T value;
    std::vector<Node *> forward;
    Node(const T& v, size_t lvl): 
        value(v), forward(lvl, nullptr) {}
    
    Node(size_t lvl):
        value(), forward(lvl, nullptr) {}
};

} // namespace templatedb
