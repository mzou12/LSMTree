#include "SSTable.hpp"
#include <unordered_set>
#include <set>
#include <algorithm>

SSTable::SSTable()
{
    size = 0;
    min = INT32_MAX;
    max = INT32_MIN;
}

SSTable::SSTable(const std::vector<Entry> &new_entries, const std::vector<RangeTomb> &new_tombs, 
    int new_min, int new_max, uint64_t new_size, uint64_t new_seq_start)
{
    entries = new_entries;
    tombs = new_tombs;
    min = new_min;
    max = new_max;
    size = new_size;
    seq_start = new_seq_start;

}


static std::vector<Fragment> build_fragments(const std::vector<RangeTomb>& tombs);

SSTable::SSTable(const std::string &filePath)
{
    path = filePath;
    std::ifstream infile(filePath);
    if (!infile.is_open()) {
        std::cerr << "Failed to open SSTable file: " << filePath << std::endl;
        return;
    }

    std::string line;
    std::getline(infile, line); size = std::stoull(line);
    std::getline(infile, line); size_t tomb_size = std::stoull(line);
    std::getline(infile, line); min = std::stoi(line);
    std::getline(infile, line); max = std::stoi(line);
    std::getline(infile, line); seq_start = std::stoull(line);

    for (size_t i = 0; i < size - tomb_size; ++i) {
        if (!std::getline(infile, line)) break;
        std::istringstream ss(line);

        uint64_t seq;
        int tomb_flag, key;
        ss >> seq >> tomb_flag >> key;

        std::vector<int> vals;
        int x;
        while (ss >> x) {
            vals.push_back(x);
        }

        if (tomb_flag) {
            entries.push_back(Entry{true, seq, key, templatedb::Value(false)});
        } else {
            entries.push_back(Entry{false, seq, key, templatedb::Value(vals)});
        }
    }

    for (size_t i = 0; i < tomb_size; ++i) {
        if (!std::getline(infile, line)) break;
        std::istringstream ss(line);
        uint64_t seq;
        int start, end;
        ss >> seq >> start >> end;
        tombs.push_back(RangeTomb{start, end, seq});
    }
    is_range_delete = !tombs.empty();
}

bool SSTable::save(const std::string& filePath)
{
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


std::optional<templatedb::Value> SSTable::get(int key)
{
    if (key > max || key < min){
        return std::nullopt;
    }
    if (is_range_delete && fragments.size() == 0){
        fragments = build_fragments(tombs);
    }
    // 二分查找第一个 key 匹配的 entry 起点
    int left = 0, right = entries.size() - 1;
    int found_idx = -1;

    while (left <= right) {
        int mid = (left + right) / 2;
        if (entries[mid].key == key) {
            found_idx = mid;
            // 还可能有更早的 key 在前面（因为多个版本按 seq 降序排列）
            right = mid - 1;
        } else if (entries[mid].key < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    if (found_idx == -1) {
        return std::nullopt; // 没有这个 key
    }

    // 遍历该 key 所有版本（从 found_idx 开始）
    for (size_t i = found_idx; i < entries.size() && entries[i].key == key; ++i) {
        const Entry& e = entries[i];

        if (e.tomb) return templatedb::Value(false); // point tombstone

        if (is_key_covered_by_fragment(key, e.seq)) return templatedb::Value(false); // range tombstone

        return e.val;
    }

    return std::nullopt;
}


void SSTable::add(int key, const templatedb::Value& val, uint64_t seq)
{
    size++;
    entries.push_back(Entry{false, seq, key, val});
    min = std::min(min, key);
    max = std::max(max, key);
}

void SSTable::point_delete(int key, uint64_t seq)
{
    size++;
    entries.push_back(Entry{true, seq, key, templatedb::Value(false)});
    min = std::min(min, key);
    max = std::max(max, key);
}

void SSTable::range_delete(int range_min, int range_max, uint64_t seq)
{
    size++;
    tombs.push_back(RangeTomb{range_min, range_max, seq});
    min = std::min(min, range_min);
    max = std::max(max, range_max);
}  

const std::vector<Entry> &SSTable::getEntries() const
{
    return entries;
}

const std::vector<RangeTomb> &SSTable::getRangeTomb() const
{
    return tombs;
}

const std::vector<Fragment> &SSTable::getFragments() const
{
    return fragments;
}

bool SSTable::hasRangeDelete()
{
    return !tombs.empty();
}

static std::vector<Fragment> build_fragments(const std::vector<RangeTomb>& tombs) {
    // 1. 收集所有边界
    std::set<int> bounds;
    for (const auto& t : tombs) {
        bounds.insert(t.start);
        bounds.insert(t.end);
    }

    std::vector<int> sorted_bounds(bounds.begin(), bounds.end());
    std::vector<Fragment> fragments;

    for (size_t i = 0; i + 1 < sorted_bounds.size(); ++i) {
        int a = sorted_bounds[i];
        int b = sorted_bounds[i + 1];

        uint64_t max_seq = 0;
        for (const auto& t : tombs) {
            if (a >= t.start && b <= t.end) {
                max_seq = std::max(max_seq, t.seq);
            }
        }

        if (max_seq > 0) {
            fragments.push_back(Fragment{a, b, max_seq});
        }
    }

    return fragments;
}

bool SSTable::is_key_covered_by_fragment(int key, uint64_t key_seq) {
    int left = 0, right = fragments.size() - 1;

    while (left <= right) {
        int mid = (left + right) / 2;
        const Fragment& frag = fragments[mid];

        if (key < frag.start) {
            right = mid - 1;  // key 在当前段左边
        } else if (key >= frag.end) {
            left = mid + 1;   // key 在当前段右边
        } else {
            // key ∈ [frag.start, frag.end)
            return frag.max_seq > key_seq;
        }
    }

    return false; // 没有命中任何 fragment
}

static bool entry_cmp(const Entry& a, const Entry& b) {
    if (a.key != b.key) return a.key < b.key;          // key 升序
    return a.seq > b.seq;                              // seq 降序（新版本在前）
}

void SSTable::sort_entries() {
    std::sort(entries.begin(), entries.end(), entry_cmp); // 如果你用 std::vector
}

static bool tomb_cmp(const RangeTomb& a, const RangeTomb& b) {
    if (a.start != b.start) return a.start < b.start; // start 升序
    return a.seq > b.seq; // 相同 start 的，先处理更新的 tombstone
}

void SSTable::sort_tombs(){
    std::sort(tombs.begin(), tombs.end(), tomb_cmp);
}

uint64_t SSTable::get_seq_start(){
    return seq_start;
}

uint64_t SSTable::get_size(){
    return size;
}

int SSTable::get_min(){
    return min;
}

int SSTable::get_max(){
    return max;
}