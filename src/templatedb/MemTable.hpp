#include <string>
#include <vector>
#include <list>
#include <cstdint>
#include <optional>

#include "templatedb/db.hpp"
#include "templatedb/SSTable.hpp"

class MemTable
{
public:
    uint64_t size = 0;
    uint64_t seq_start = 0;
    int min = 0, max;
    MemTable();
    bool save(const std::string& filePath);

    std::optional<templatedb::Value> get(int key);
    void add(int key, const templatedb::Value& val, uint64_t seq);
    void point_delete(int key, uint64_t seq);
    void range_delete(int min, int max, uint64_t seq);
    const std::vector<Entry>& getEntries() const;
    const std::vector<RangeTomb>& getRangeTomb() const;
    bool hasRangeDelete();

    void sort_entries();
    void sort_tombs();
    void clear();

private:
    std::vector<Entry> entries;
    std::vector<RangeTomb> tombs;
};
