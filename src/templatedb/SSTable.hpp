#pragma once
#include <string>
#include <vector>
#include <list>
#include <cstdint>
#include <optional>

// #include "../BloomFilter/BloomFilter.h"
#include "templatedb/db.hpp"

struct Entry
{
    bool tomb = false;
    uint64_t seq;
    int key;
    templatedb::Value val;
};

struct RangeTomb
{
    int start, end;
    uint64_t seq;
};

struct Fragment
{
    int start, end;
    uint64_t max_seq;
};

class SSTable
{
public:
    // BF::BloomFilter *bloomFilter;
    SSTable();
    SSTable(const std::vector<Entry>& entries,
        const std::vector<RangeTomb>& tombs,
        int min, int max,
        uint64_t size, uint64_t seq_start);
    SSTable(const std::string& filePath);
    bool save(const std::string& filePath);

    std::optional<templatedb::Value> get(int key);
    void add(int key, const templatedb::Value& val, uint64_t seq);
    void point_delete(int key, uint64_t seq);
    void range_delete(int min, int max, uint64_t seq);
    const std::vector<Entry>& getEntries() const;
    const std::vector<RangeTomb>& getRangeTomb() const;
    const std::vector<Fragment>& getFragments() const;
    bool hasRangeDelete();
    bool is_key_covered_by_fragment(int key, uint64_t key_seq);

    void sort_entries();
    void sort_tombs();
    uint64_t get_seq_start();
    uint64_t get_size();
    int get_min();
    int get_max();

private:
    std::vector<Entry> entries;
    std::vector<RangeTomb> tombs;
    std::vector<Fragment> fragments;
    bool is_range_delete = false;
    uint64_t size = 0;
    uint64_t seq_start = 0;
    int min = 0, max;
    std::string path;
};
