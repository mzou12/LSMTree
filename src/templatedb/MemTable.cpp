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

MemTable::MemTable(const std::vector<templatedb::Entry> &new_entries, const std::vector<templatedb::RangeTomb> &new_tombs, 
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
    sort_entries();
    sort_tombs();
    return save(filePath);
}


bool MemTable::save(const std::string &filePath)
{
    std::ofstream file(filePath, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) return false;

    // Step 1: header
    file << size << "\n";
    file << tombs.size() << "\n";
    file << min << "\n";
    file << max << "\n";
    file << seq_start << "\n";
    file << 10 << "\n"; //bitsPerElements

    std::streampos offset_pos = file.tellp();

    // 5 line, entry_offset, tomb_offset, key_index_offset, num_elements, bloom_filter_offset
    for (int i = 0; i < 5; ++i) {
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

    std::streampos bloom_filter_offset = file.tellp();
    BF::BloomFilter bloom_filter(seen.size(), 10);
    for (int key: seen){
        bloom_filter.program(std::to_string(key));
    }
    for (bool bit : bloom_filter.bf_vec) {
        file << (bit ? '1' : '0');
    }
    file << "\n";

    // Step 4: key-offset table
    std::streampos key_index_offset = file.tellp();
    for (const auto& [key, pos] : key_offsets) {
        file << key << " " << static_cast<uint64_t>(pos) << "\n";
    }

    // Step 5: rewrite to header
    file.seekp(offset_pos);
    file << std::setw(10) << std::left << entry_offset << "\n"
         << std::setw(10) << std::left << tomb_offset << "\n"
         << std::setw(10) << std::left << key_index_offset << "\n"
         << std::setw(10) << std::left << seen.size() << "\n" // BLOOMFILTER numElements
         << std::setw(10) << std::left << bloom_filter_offset << "\n" ;

    file.close();
    return true;
}


std::optional<templatedb::Value> MemTable::get(int key)
{
    if (key > max || key < min){
        return std::nullopt;
    }
    templatedb::Entry* best = nullptr;
    for (auto entry = entries.rbegin(); entry != entries.rend(); ++entry){
        if (entry->key == key) {
            if (!best || entry->seq > best->seq) {
                best = &(*entry);
            }
        }
    }
    for (const auto& t : tombs) {
        if (key >= t.start && key < t.end) {
            if (!best){
                return templatedb::Value(false); 
            } else if (t.seq > best->seq){
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
    entries.push_back(templatedb::Entry{false, seq, key, val});
    if (seq_start == -1)
        seq_start = seq;
    sorted = false;
    min = std::min(min, key);
    max = std::max(max, key);
}

void MemTable::point_delete(int key, uint64_t seq)
{
    size++;
    entries.push_back(templatedb::Entry{true, seq, key, templatedb::Value(false)});
    if (seq_start == -1)
        seq_start = seq;
    sorted = false;
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

static bool entry_cmp(const templatedb::Entry& a, const templatedb::Entry& b) {
    if (a.key != b.key) return a.key < b.key;          // key increase
    return a.seq > b.seq;                              // seq decrease
}

void MemTable::sort_entries() {
    std::sort(entries.begin(), entries.end(), entry_cmp);
}


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
    sorted_entries.clear();
    sorted_tombs.clear();
    iter_index = 0;
    range_iter_index = 0;
    sorted = false;
    range_sorted = false;
}

std::optional<templatedb::Entry> MemTable::next(){
    if (!has_next()){
        return std::nullopt;
    }
    return sorted_entries[iter_index++];
}

bool MemTable::has_next(){
    return iter_index < sorted_entries.size();
}

void MemTable::reset_iterator(){
    iter_index = 0;
    if (!sorted){
        sorted_entries = entries;
        std::sort(sorted_entries.begin(), sorted_entries.end(), entry_cmp);
        sorted = true;
    }
}

std::optional<templatedb::RangeTomb> MemTable::range_tombs_next(){
    if (!range_tombs_has_next()) return std::nullopt;
    return sorted_tombs[range_iter_index++];
}

bool MemTable::range_tombs_has_next(){
    return range_iter_index < (tombs.size());
}

void MemTable::reset_range_iterator(){
    range_iter_index = 0;
    if (!range_sorted){
        sorted_tombs = tombs;
        std::sort(sorted_tombs.begin(), sorted_tombs.end(), tomb_cmp);
        range_sorted = true;
    }
}