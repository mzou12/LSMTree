#include "MemTable.hpp"
#include <algorithm>
#include <unordered_set>
#include <iomanip>
#include <sstream>

MemTable::MemTable()
{
    size = 0;
    min = INT32_MAX;
    max = INT32_MIN;
}

MemTable::MemTable(const SkipList<int, templatedb::Entry> &new_entries, const std::vector<templatedb::RangeTomb> &new_tombs, 
    int new_min, int new_max, uint64_t new_size, uint64_t new_seq_start)
{
    entries = new_entries;
    tombs = new_tombs;
    min = new_min;
    max = new_max;
    size = new_size;
    seq_start = new_seq_start;

}

bool MemTable::flush(const std::string &filePath)
{
    if (!range_sorted){
        sort_tombs();
    }
    return save(filePath);
}

bool MemTable::save(const std::string &filePath)
{
    std::ofstream file(filePath, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) return false;

    // header
    file << size << "\n";
    file << tombs.size() << "\n";
    file << min << "\n";
    file << max << "\n";
    file << seq_start << "\n";

    std::streampos offset_pos = file.tellp();

    // 3 entry offset tomb offset key_index_offset
    for (int i = 0; i < 3; ++i) {
        file << "          \n";  // 10 spaces + \n
    }

    // Step 2: entries
    std::vector<std::pair<int, std::streampos>> key_offsets;
    std::unordered_set<int> seen;
    std::streampos entry_offset = file.tellp();

    for (const auto& e : entries) {
        std::streampos pos = file.tellp();
        if (!seen.count(e.key)) {
            key_offsets.push_back({e.key, pos});
            seen.insert(e.key);
        }
        file << e.seq << " " << (e.tomb ? 1 : 0) << " " << e.key;
        for (int v : e.val.items) file << " " << v;
        file << "\n";
    }

    // Step 3: tombstones
    std::streampos tomb_offset = file.tellp();
    for (const auto& t : tombs) {
        file << t.seq << " " << t.start << " " << t.end << "\n";
    }

    // Step 4: key-offset table
    std::streampos key_index_offset = file.tellp();
    for (const auto& [key, pos] : key_offsets) {
        file << key << " " << static_cast<uint64_t>(pos) << "\n";
    }

    // Step 5: 回写 header offset 部分（覆盖写，每行最多写 10 字符 + \n）
    file.seekp(offset_pos);
    file << std::setw(10) << std::left << entry_offset << "\n"
         << std::setw(10) << std::left << tomb_offset << "\n"
         << std::setw(10) << std::left << key_index_offset << "\n";

    file.close();
    return true;
}


std::optional<templatedb::Value> MemTable::get(int key)
{
    if (key > max || key < min){
        return std::nullopt;
    }
    std::optional<templatedb::Entry> best = entries.find(key);
    // for (auto entry = entries.rbegin(); entry != entries.rend(); ++entry){
    //     if (entry->key == key) {
    //         if (!best || entry->seq > best->seq) {
    //             best = &(*entry);
    //         }
    //     }
    // }
    for (const auto& t : tombs) {
        if (key >= t.start && key < t.end) {
            if (!best.has_value()){
                return templatedb::Value(false); 
            } else if (t.seq > best.value().seq){
                return templatedb::Value(false); 
            }
        }
    }
    if (!best) return std::nullopt;
    if (best->tomb) return templatedb::Value(false);
    return best->val;
}

void MemTable::add(int key, const templatedb::Value &val, uint64_t seq)
{
    size++;
    entries.insert(key, templatedb::Entry{false, seq, key, val});
    if (seq_start == -1)
        seq_start = seq;
    min = std::min(min, key);
    max = std::max(max, key);
}

void MemTable::point_delete(int key, uint64_t seq)
{
    size++;
    entries.insert(key, templatedb::Entry{true, seq, key, templatedb::Value(false)});
    if (seq_start == -1)
        seq_start = seq;
    min = std::min(min, key);
    max = std::max(max, key);
}

void MemTable::range_delete(int range_min, int range_max, uint64_t seq)
{
    size++;
    tombs.push_back(templatedb::RangeTomb{range_min, range_max, seq});
    if (seq_start == -1)
        seq_start = seq;
    range_sorted = false;
    min = std::min(min, range_min);
    max = std::max(max, range_max);
}

bool MemTable::hasRangeDelete()
{
    return !tombs.empty();
}

// static bool entry_cmp(const templatedb::Entry& a, const templatedb::Entry& b) {
//     if (a.key != b.key) return a.key < b.key;          // key increase
//     return a.seq > b.seq;                              // seq decrease for newer version
// }

// void MemTable::sort_entries() {
//     std::sort(entries.begin(), entries.end(), entry_cmp); 
// }


static bool tomb_cmp(const templatedb::RangeTomb& a, const templatedb::RangeTomb& b) {
    if (a.start != b.start) return a.start < b.start;
    return a.seq > b.seq; 
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
    range_sorted = false;
}

std::optional<templatedb::Entry> MemTable::next(){
    if (!entry_iter.hasNext()){
        return std::nullopt;
    }
    return entry_iter.next();
}

bool MemTable::has_next(){
    return entry_iter.hasNext();
}

void MemTable::reset_iterator(){
    entry_iter = entries.begin();
}

std::optional<templatedb::RangeTomb> MemTable::range_tombs_next(){
    if (!range_tombs_has_next()) return std::nullopt;
    return *(rt_iter++);
}

bool MemTable::range_tombs_has_next(){
    return rt_iter != tombs.end();
}

void MemTable::reset_range_iterator(){
    if (!range_sorted){
        sort_tombs();
        range_sorted = true;
    }
    rt_iter = tombs.begin();
}