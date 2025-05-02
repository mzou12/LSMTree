#include "SSTable.hpp"
#include <unordered_set>
#include <set>
#include <algorithm>
#include <iomanip>

SSTable::SSTable()
{
    size = 0;
    min = INT32_MAX;
    max = INT32_MIN;
}

SSTable::SSTable(const std::vector<templatedb::Entry> &new_entries, const std::vector<templatedb::RangeTomb> &new_tombs, 
    int new_min, int new_max, uint64_t new_size, uint64_t new_seq_start)
{
    entries = new_entries;
    tombs = new_tombs;
    min = new_min;
    max = new_max;
    size = new_size;
    seq_start = new_seq_start;

}

void SSTable::load_key_offset()
{
    if (read_offset) return;
    read_offset = true;
    infile.clear();
    infile.seekg(key_index_offset);
    std::string line;
    while (std::getline(infile, line)) {
        std::istringstream ss(line);
        int key;
        uint64_t raw_offset;
        ss >> key >> raw_offset;
        std::streampos offset = static_cast<std::streampos>(raw_offset);
        key_offsets.push_back({key, offset});
    }

}


static std::vector<templatedb::Fragment> build_fragments(const std::vector<templatedb::RangeTomb>& tombs);

SSTable::SSTable(const std::string &filePath)
{
    path = filePath;
    infile.open(filePath);
    if (!infile.is_open()) {
        std::cerr << "Failed to open SSTable file: " << filePath << std::endl;
        return;
    }

    std::string line;
    std::getline(infile, line); size = std::stoull(line);
    std::getline(infile, line); tombs_size = std::stoull(line);
    std::getline(infile, line); min = std::stoi(line);
    std::getline(infile, line); max = std::stoi(line);
    std::getline(infile, line); seq_start = std::stoull(line);
    std::getline(infile, line); entry_offset = std::stoull(line);
    std::getline(infile, line); tomb_offset = std::stoull(line);
    std::getline(infile, line); key_index_offset = std::stoull(line);

    is_range_delete = (tombs_size > 0);
    infile.seekg(key_index_offset);
    while (std::getline(infile, line)) {
        std::istringstream ss(line);
        int key;
        uint64_t raw_offset;
        ss >> key >> raw_offset;
        std::streampos offset = static_cast<std::streampos>(raw_offset);
        key_offsets.push_back({key, offset});
    }
}

bool SSTable::save(const std::string& filePath)
{
    std::ofstream file(filePath, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) return false;

    // Step 1: header
    file << size << "\n";
    file << tombs.size() << "\n";
    file << min << "\n";
    file << max << "\n";
    file << seq_start << "\n";

    std::streampos offset_pos = file.tellp();

    // entry offset, tomb offset, key index offset
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

    // write back to the offset
    file.seekp(offset_pos);
    file << std::setw(10) << std::left << entry_offset << "\n"
         << std::setw(10) << std::left << tomb_offset << "\n"
         << std::setw(10) << std::left << key_index_offset << "\n";

    file.close();
    return true;
}

void SSTable::load_tombs() {
    if (!is_range_delete || !tombs.empty()){
        return;
    }

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return;

    file.seekg(tomb_offset);

    std::string line;
    for (size_t i = 0; i < tombs_size && std::getline(file, line); ++i) {
        std::istringstream ss(line);
        uint64_t seq;
        int start, end;
        ss >> seq >> start >> end;
        tombs.push_back(templatedb::RangeTomb{start, end, seq});
    }
}

std::optional<templatedb::Value> SSTable::get(int key)
{
    if (key > max || key < min){
        return std::nullopt;
    }
    // binary search
    if (!read_offset){
        load_key_offset();
    }
    int left = 0, right = key_offsets.size() - 1;
    int found_idx = -1;

    while (left <= right) {
        int mid = (left + right) / 2;
        if (key_offsets[mid].first == key) {
            found_idx = mid;
            break;
        } else if (key_offsets[mid].first < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    if (is_range_delete && fragments.empty()){
        load_tombs();
        fragments = build_fragments(tombs);
    }
    if (found_idx != -1){
        if(!infile.is_open()){
            infile.open(path);
        }
        infile.clear();
        infile.seekg(key_offsets[found_idx].second);
        std::string line;
        while (std::getline(infile, line)) {
            templatedb::Entry e = parse_line(line);
            if (e.key != key) break;

            if (is_range_delete && is_key_covered_by_fragment(key, e.seq)) return templatedb::Value(false);
            return e.val;
        }
    }
    if (is_range_delete&&is_key_covered_by_fragment(key, 0)){
        return templatedb::Value(false);
    }

    return std::nullopt;
}


// void SSTable::add(int key, const templatedb::Value& val, uint64_t seq)
// {
//     size++;
//     entries.push_back(templatedb::Entry{false, seq, key, val});
//     min = std::min(min, key);
//     max = std::max(max, key);
// }

// void SSTable::point_delete(int key, uint64_t seq)
// {
//     size++;
//     entries.push_back(templatedb::Entry{true, seq, key, templatedb::Value(false)});
//     min = std::min(min, key);
//     max = std::max(max, key);
// }

// void SSTable::range_delete(int range_min, int range_max, uint64_t seq)
// {
//     size++;
//     tombs.push_back(templatedb::RangeTomb{range_min, range_max, seq});
//     min = std::min(min, range_min);
//     max = std::max(max, range_max);
// }  

void SSTable::load_entries(){
    if (!entries.empty()){
        return;
    }
    std::string line;
    infile.clear();
    infile.seekg(entry_offset);
    for (size_t i = 0; i < size - tombs_size; ++i) {
        if (!std::getline(infile, line)) 
            break;
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
            entries.push_back(templatedb::Entry{true, seq, key, templatedb::Value(false)});
        } else {
            entries.push_back(templatedb::Entry{false, seq, key, templatedb::Value(vals)});
        }
    }
}

std::vector<templatedb::Entry> &SSTable::getEntries()
{
    load_entries();
    return entries;
}

std::vector<templatedb::RangeTomb> &SSTable::getRangeTomb()
{
    load_tombs();
    return tombs;
}

const std::vector<templatedb::Fragment> &SSTable::getFragments() const
{
    return fragments;
}

bool SSTable::hasRangeDelete()
{
    return !tombs.empty();
}

static std::vector<templatedb::Fragment> build_fragments(const std::vector<templatedb::RangeTomb>& tombs) {
    // collect bonds
    std::set<int> bounds;
    for (const auto& t : tombs) {
        bounds.insert(t.start);
        bounds.insert(t.end);
    }

    std::vector<int> sorted_bounds(bounds.begin(), bounds.end());
    std::vector<templatedb::Fragment> fragments;

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
            fragments.push_back(templatedb::Fragment{a, b, max_seq});
        }
    }

    return fragments;
}

bool SSTable::is_key_covered_by_fragment(int key, uint64_t key_seq) {
    int left = 0, right = fragments.size() - 1;

    while (left <= right) {
        int mid = (left + right) / 2;
        const templatedb::Fragment& frag = fragments[mid];

        if (key < frag.start) {
            right = mid - 1;  // key in the left
        } else if (key >= frag.end) {
            left = mid + 1;   // key in the right
        } else {
            // key [frag.start, frag.end)
            return frag.max_seq > key_seq;
        }
    }

    return false; // no hit
}

static bool entry_cmp(const templatedb::Entry& a, const templatedb::Entry& b) {
    if (a.key != b.key) return a.key < b.key;
    return a.seq > b.seq;
}

void SSTable::sort_entries() {
    std::sort(entries.begin(), entries.end(), entry_cmp); 
}

static bool tomb_cmp(const templatedb::RangeTomb& a, const templatedb::RangeTomb& b) {
    if (a.start != b.start) return a.start < b.start; 
    return a.seq > b.seq; 
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

templatedb::Entry SSTable::parse_line(const std::string& line) {
    std::istringstream ss(line);
    uint64_t seq;
    int tomb_flag, key;
    ss >> seq >> tomb_flag >> key;
    std::vector<int> items;
    int x;
    while (ss >> x) items.push_back(x);
    if (tomb_flag) {
        return templatedb::Entry{true, seq, key, templatedb::Value(false)};
    } 
    return templatedb::Entry{false, seq, key, templatedb::Value(items)};
}

std::optional<templatedb::Entry> SSTable::next()
{
    if (!has_next()) 
        return std::nullopt;
    infile.clear();
    infile.seekg(key_offsets[iter_index].second);
    std::string line;
    if (!std::getline(infile, line)) 
        return std::nullopt;
    
    templatedb::Entry e = parse_line(line);
    iter_index++;
    // std::cout << "[DEBUG] SSTable::next() idx=" << iter_index << "\n";
    // std::cout << "key offset"<< key_offsets.size() <<"\n";
    return e;
}

void SSTable::reset_iterator()
{
    iter_index = 0;
}

bool SSTable::has_next()
{
    return iter_index < key_offsets.size();
}

std::optional<templatedb::RangeTomb> SSTable::range_tombs_next(){
    if (!range_tombs_has_next()) return std::nullopt;

    std::string line;
    if (!std::getline(tombfile, line)) return std::nullopt;

    std::istringstream ss(line);
    uint64_t seq;
    int start, end;
    ss >> seq >> start >> end;

    ++range_iter_index;
    return templatedb::RangeTomb{start, end, seq};
};

bool SSTable::range_tombs_has_next(){
    return range_iter_index < tombs_size;
};

void SSTable::reset_range_iterator(){
    if (tombfile.is_open()){
        tombfile.close();
    }
    tombfile.open(path, std::ios::binary);
    tombfile.clear();
    tombfile.seekg(tomb_offset);
    range_iter_index = 0;
};

