#include <string>
#include <vector>
#include <list>
#include <cstdint>
#include <optional>
#include <iostream>
#include <fstream>

#include "struct.hpp"
#include "SkipList.hpp"

class MemTable
{
public:
    uint64_t size = 0;
    uint64_t seq_start = -1;
    int min = 0, max;
    MemTable();
    MemTable(const SkipList<int, templatedb::Entry>& entries,
        const std::vector<templatedb::RangeTomb>& tombs,
        int min, int max,
        uint64_t size, uint64_t seq_start);
    bool flush(const std::string& filePath);
    bool save(const std::string& filePath);

    std::optional<templatedb::Value> get(int key);
    void add(int key, const templatedb::Value& val, uint64_t seq);
    void point_delete(int key, uint64_t seq);
    void range_delete(int min, int max, uint64_t seq);
    bool hasRangeDelete();

    void sort_tombs();
    void clear();
    std::optional<templatedb::Entry> next();
    bool has_next();
    void reset_iterator();
    std::optional<templatedb::RangeTomb> range_tombs_next();
    bool range_tombs_has_next();
    void reset_range_iterator();

private:
    SkipList<int, templatedb::Entry> entries;
    std::vector<templatedb::RangeTomb> tombs;
    SkipList<int, templatedb::Entry>::Iterator entry_iter;
    std::vector<templatedb::RangeTomb>::iterator rt_iter;
    bool range_sorted = false;
    
};
