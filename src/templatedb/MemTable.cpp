#include "MemTable.hpp"
#include <algorithm>

MemTable::MemTable()
{
    size = 0;
    min = INT32_MAX;
    max = INT32_MIN;
}

bool MemTable::save(const std::string &filePath)
{
    sort_entries();
    sort_tombs();
    std::ofstream file(filePath);
    if (!file.is_open()) return false;

    file << size << "\n";
    file << tombs.size() << "\n";
    file << min << "\n";
    file << max << "\n";
    file << seq_start << "\n";

    for (const auto& e : entries) {
        file << e.seq << " " << (e.tomb ? 1 : 0) << " " << e.key;
        for (int v : e.val.items) {
            file << " " << v;
        }
        file << "\n";
    }

    for (const auto& t : tombs) {
        file << t.seq << " " << t.start << " " << t.end << "\n";
    }
    return true;
}

std::optional<templatedb::Value> MemTable::get(int key)
{
    if (key > max || key < min){
        return std::nullopt;
    }
    Entry* best = nullptr;
    for (auto entry = entries.rbegin(); entry != entries.rend(); ++entry){
        if (entry->key == key) {
            if (!best || entry->seq > best->seq) {
                best = &(*entry);
            }
        }
    }
    if (!best) return std::nullopt;
    if (best->tomb) return templatedb::Value(false);
    for (const auto& t : tombs) {
        if (key >= t.start && key < t.end && t.seq > best->seq) {
            return templatedb::Value(false); 
        }
    }
    return best->val;
}

void MemTable::add(int key, const templatedb::Value &val, uint64_t seq)
{
    size++;
    entries.push_back(Entry{false, seq, key, val});
    if (seq_start == -1)
        seq_start = seq;
    min = std::min(min, key);
    max = std::max(max, key);
}

void MemTable::point_delete(int key, uint64_t seq)
{
    size++;
    entries.push_back(Entry{true, seq, key, templatedb::Value(false)});
    if (seq_start == -1)
        seq_start = seq;
    min = std::min(min, key);
    max = std::max(max, key);
}

void MemTable::range_delete(int range_min, int range_max, uint64_t seq)
{
    size++;
    tombs.push_back(RangeTomb{range_min, range_max, seq});
    if (seq_start == -1)
        seq_start = seq;
    min = std::min(min, range_min);
    max = std::max(max, range_max);
}

bool MemTable::hasRangeDelete()
{
    return !tombs.empty();
}

static bool entry_cmp(const Entry& a, const Entry& b) {
    if (a.key != b.key) return a.key < b.key;          // key 升序
    return a.seq > b.seq;                              // seq 降序（新版本在前）
}

void MemTable::sort_entries() {
    std::sort(entries.begin(), entries.end(), entry_cmp); // 如果你用 std::vector
}


static bool tomb_cmp(const RangeTomb& a, const RangeTomb& b) {
    if (a.start != b.start) return a.start < b.start; // start 升序
    return a.seq > b.seq; // 相同 start 的，先处理更新的 tombstone
}

void MemTable::sort_tombs(){
    std::sort(tombs.begin(), tombs.end(), tomb_cmp);
}


void MemTable::clear(){
    size = 0;
    min = INT32_MAX;
    max = INT32_MIN;
    seq_start = -1;
    entries.clear();
    tombs.clear();

}