#pragma once
#include <string>
#include <vector>
#include <list>
#include <cstdint>
#include <optional>
#include <iostream>
#include <fstream>

#include "BloomFilter.h"
#include "struct.hpp"

class SSTable
{
public:
    SSTable();
    SSTable(const std::string& filePath);
    // bool save(const std::string& filePath);

    std::optional<templatedb::Value> get(int key);
    
    // void add(int key, const templatedb::Value& val, uint64_t seq);
    // void point_delete(int key, uint64_t seq);
    // void range_delete(int min, int max, uint64_t seq);
    std::vector<templatedb::Entry>& getEntries();
    std::vector<templatedb::RangeTomb>& getRangeTomb();
    const std::vector<templatedb::Fragment>& getFragments() const;
    bool hasRangeDelete();
    bool is_key_covered_by_fragment(int key, uint64_t key_seq);

    void sort_entries();
    void sort_tombs();
    uint64_t get_seq_start();
    uint64_t get_size();
    int get_min();
    int get_max();
    templatedb::Entry parse_line(const std::string &line);
    std::optional<templatedb::Entry> next();
    bool has_next();
    void reset_iterator();
    std::optional<templatedb::RangeTomb> range_tombs_next();
    bool range_tombs_has_next();
    void reset_range_iterator();
    void load_key_offset();

private:
    std::vector<templatedb::Entry> entries;
    std::vector<templatedb::RangeTomb> tombs;
    std::vector<templatedb::Fragment> fragments;
    bool is_range_delete = false;
    bool key_offset_generate = false;
    bool use_bloom = false;
    uint64_t size = 0;
    uint64_t tombs_size = 0;
    uint64_t seq_start = 0;
    int min = 0, max;
    int iter_index = 0;
    int range_iter_index = 0;
    std::string path;
    std::ifstream infile;
    std::ifstream tombfile;
    std::streampos entry_offset;
    std::streampos tomb_offset;
    std::streamoff key_index_offset;
    std::vector<std::pair<int, std::streampos>> key_offsets;
    BF::BloomFilter bloom_filter;
    void load_tombs();
    void load_entries();
};
