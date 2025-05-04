#include "db.hpp"
#include <cmath>
#include <set>
#include <algorithm>
#include <unordered_set>
#include <queue>

using namespace templatedb;

templatedb::DB::DB()
{
    sstables_file.push_back({});
    levels_size.push_back(0);
}

Value DB::get(int key)
{
    if (mmt.get(key).has_value()){
        return mmt.get(key).value();
    }
    for (int i = 0; i <= max_level; i++){
        if (sstables_file.at(i).size() == 0){
            continue;
        }
        for (int j = sstables_file.at(i).size()-1; j>=0; j--){
            std::string path = path_control(i, j);
            std::optional<Value> check = SSTable(path).get(key);
            if (check.has_value()){
                return check.value();
            }
        }
    }
    return Value(false);
}


void DB::put(int key, Value val)
{
    mmt.add(key, val, seq);
    seq++;
    count+=1;
    db_size += 1;
    flush_check();
}


static bool entry_cmp(const Entry& a, const Entry& b) {
    if (a.key != b.key) return a.key < b.key;          // key increase
    return a.seq > b.seq;                              // seq decrease
}

static bool tomb_cmp(const RangeTomb& a, const RangeTomb& b) {
    if (a.start != b.start) return a.start < b.start; // start increase
    return a.seq > b.seq; 
}


static std::vector<Fragment> build_fragments(const std::vector<RangeTomb>& tombs) {
    //  collect bond
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

static bool is_key_covered_by_fragment(const std::vector<Fragment>& fragments, int key, uint64_t key_seq){
    int left = 0, right = fragments.size() - 1;

    while (left <= right) {
        int mid = (left + right) / 2;
        const Fragment& frag = fragments[mid];

        if (key < frag.start) {
            right = mid - 1;  // key in the left
        } else if (key >= frag.end) {
            left = mid + 1;   // key right
        } else {
            // key [frag.start, frag.end)
            return frag.max_seq > key_seq;
        }
    }

    return false;
}


std::vector<Value> DB::scan() {
    std::vector<Value> result;
    std::unordered_set<int> seen_keys;

    // build fragments
    std::vector<RangeTomb> tombs;

    mmt.reset_range_iterator();
    while (mmt.range_tombs_has_next())
        tombs.push_back(mmt.range_tombs_next().value());

    std::vector<SSTable> sstables;
    for (int i = 0; i <= max_level; ++i) {
        for (int j = sstables_file[i].size() - 1; j >= 0; --j) {
            std::string path = path_control(i, j);
            sstables.emplace_back(path);
            sstables.back().reset_range_iterator();
            while (sstables.back().range_tombs_has_next())
                tombs.push_back(sstables.back().range_tombs_next().value());
        }
    }

    std::vector<Fragment> fragments = build_fragments(tombs);

    // iterator heap
    struct Item {
        Entry entry;
        int source; // 0 = MemTable, 1~n = SSTable[i-1]
    };

    auto cmp = [](const Item& a, const Item& b) {
        return a.entry.key > b.entry.key; // min-heap
    };

    std::priority_queue<Item, std::vector<Item>, decltype(cmp)> pq(cmp);

    mmt.reset_iterator();
    std::vector<bool> is_mmt = {true};
    if (mmt.has_next())
        pq.push({mmt.next().value(), 0});

    for (int i = 0; i < sstables.size(); ++i) {
        sstables[i].reset_iterator();
        is_mmt.push_back(false);
        if (sstables[i].has_next())
            pq.push({sstables[i].next().value(), i + 1});
    }

    // heap merge
    while (!pq.empty()) {
        Item item = pq.top(); pq.pop();
        Entry& e = item.entry;
        int src = item.source;

        if (seen_keys.count(e.key)) {
            // skip remaining versions of the same key
        } else {
            seen_keys.insert(e.key);

            if (!e.tomb && !is_key_covered_by_fragment(fragments, e.key, e.seq)) {
                result.push_back(e.val);
            }
        }

        // Push next
        if (is_mmt[src]) {
            if (mmt.has_next())
                pq.push({mmt.next().value(), src});
        } else {
            if (sstables[src - 1].has_next())
                pq.push({sstables[src - 1].next().value(), src});
        }
    }

    return result;
}




std::vector<Value> DB::scan(int min_key, int max_key) {
    std::vector<Value> result;
    std::unordered_set<int> seen_keys;
    std::vector<RangeTomb> tombs;

    // build fragments
    mmt.reset_range_iterator();
    while (mmt.range_tombs_has_next())
        tombs.push_back(mmt.range_tombs_next().value());

    std::vector<SSTable> sstables;
    for (int i = 0; i <= max_level; ++i) {
        for (int j = sstables_file[i].size() - 1; j >= 0; --j) {
            std::string path = path_control(i, j);
            sstables.emplace_back(path);
            sstables.back().reset_range_iterator();
            while (sstables.back().range_tombs_has_next())
                tombs.push_back(sstables.back().range_tombs_next().value());
        }
    }

    std::vector<Fragment> fragments = build_fragments(tombs);

    // build iterator
    struct Item {
        Entry entry;
        int source_id; // 0 = MemTable, 1~n = SSTable[i-1]
    };

    auto cmp = [](const Item& a, const Item& b) {
        return a.entry.key > b.entry.key; // min-heap
    };
    std::priority_queue<Item, std::vector<Item>, decltype(cmp)> pq(cmp);

    mmt.reset_iterator();
    std::vector<bool> is_mmt = {true};
    if (mmt.has_next())
        pq.push({mmt.next().value(), 0});

    for (int i = 0; i < sstables.size(); ++i) {
        sstables[i].reset_iterator();
        is_mmt.push_back(false);
        if (sstables[i].has_next())
            pq.push({sstables[i].next().value(), i + 1});
    }

    // Scan and merge, and filter the valid values within the range of min_key to max_key
    while (!pq.empty()) {
        Item item = pq.top(); pq.pop();
        Entry& e = item.entry;
        int src = item.source_id;

        // right bond
        if (e.key >= max_key)
            break;

        // left bond
        if (e.key < min_key) {
            // no count for search newer version
        } else if (!seen_keys.count(e.key)) {
            seen_keys.insert(e.key);

            if (!e.tomb && !is_key_covered_by_fragment(fragments, e.key, e.seq)) {
                result.push_back(e.val);
            }
        }

        // iterator
        if (is_mmt[src]) {
            if (mmt.has_next())
                pq.push({mmt.next().value(), src});
        } else {
            if (sstables[src - 1].has_next())
                pq.push({sstables[src - 1].next().value(), src});
        }
    }

    return result;
}




void DB::del(int key)
{
    mmt.point_delete(key, seq);
    seq++;
    count += 1;
    db_size += 1;
    flush_check();
}

// delte from [min,max), notice not [4,6]!!!!
void DB::del(int min_key, int max_key)
{
    mmt.range_delete(min_key, max_key, seq);
    seq++;
    count += 1;
    db_size += 1;
    flush_check();
}


size_t DB::size()
{
    return db_size;
}


std::vector<Value> DB::execute_op(Operation op)
{
    std::vector<Value> results;
    if (op.type == GET)
    {
        results.push_back(this->get(op.key));
    }
    else if (op.type == PUT)
    {
        this->put(op.key, Value(op.args));
    }
    else if (op.type == SCAN)
    {
        results = this->scan(op.key, op.args[0]);
    }
    else if (op.type == DELETE)
    {
        if ( op.args.size()>0 ){
            this->del(op.key, op.args[0]);
        }
        else
            this->del(op.key);
    }

    return results;
}


bool DB::load_data_file(std::string & fname)
{
    std::ifstream fid(fname);
    if (fid.is_open())
    {
        int key;
        int line_num = 0;
        std::string line;
        std::getline(fid, line); // First line is rows, col
        while (std::getline(fid, line))
        {
            line_num++;
            std::stringstream linestream(line);
            std::string item;

            std::getline(linestream, item, ' ');
            std::string op_code = item;

            std::getline(linestream, item, ' ');
            key = stoi(item);
            std::vector<int> items;
            while(std::getline(linestream, item, ' '))
            {
                items.push_back(stoi(item));
            }
            this->put(key, Value(items));
        }
    }
    else
    {
        fprintf(stderr, "Unable to read %s\n", fname.c_str());
        return false;
    }

    return true;
}

db_status DB::open(std::string & fname)
{
    this->file.open(fname, std::ios::in | std::ios::out);
    if (file.is_open())
    {
        this->status = OPEN;
        // New file implies empty file
        if (file.peek() == std::ifstream::traits_type::eof())
            return this->status;

        int key;
        std::string line;
        std::getline(file, line); // First line is rows, col
        while (std::getline(file, line))
        {
            std::stringstream linestream(line);
            std::string item;

            std::getline(linestream, item, ',');
            key = stoi(item);
            std::vector<int> items;
            while(std::getline(linestream, item, ','))
            {
                items.push_back(stoi(item));
            }
            this->put(key, Value(items));
            if (value_dimensions == 0)
                value_dimensions = items.size();
        }
    }
    else if (!file) // File does not exist
    {
        this->file.open(fname, std::ios::out);
        this->status = OPEN;
    }
    else
    {
        file.close();
        this->status = ERROR_OPEN;
    }

    return this->status; 
}


bool DB::close()
{
    if (file.is_open())
    {
        // this->write_to_file();
        file.close();
    }
    this->status = CLOSED;

    return true;
}


// bool DB::write_to_file()
// {
//     file.clear();
//     file.seekg(0, std::ios::beg);

//     std::string header = std::to_string(table.size()) + ',' + std::to_string(value_dimensions) + '\n';
//     file << header;
//     for(auto item: table)
//     {
//         std::ostringstream line;
//         std::copy(item.second.items.begin(), item.second.items.end() - 1, std::ostream_iterator<int>(line, ","));
//         line << item.second.items.back();
//         std::string value(line.str());
//         file << item.first << ',' << value << '\n';
//     }

//     return true;
// }


void templatedb::DB::flush()
{
    int sst_num = sstables_file.at(0).size();
    std::string path = path_control(0, sst_num);
    sstables_file.at(0).push_back(sst_num); // Add file's num
    mmt.flush(path);
    levels_size.at(0) += flush_base;
    if (levels_size.at(0) >= level_size_base){
        compact(0);
    }
}

bool templatedb::DB::flush_check()
{
    if (count >= flush_base){
        flush();
        count = 0;
        mmt.clear();
        return true;
    }
    return false;
}

void templatedb::DB::compact(int level) {
    int this_level_size = levels_size.at(level);

    // Todo: tiering, compact this to next level
    std::vector<Entry> entries;
    std::vector<RangeTomb> tombs;

    int last_idx = sstables_file.at(level).size() - 1;
    std::string file_path = path_control(level, sstables_file.at(level).at(last_idx));
    SSTable sst(file_path);
    uint64_t new_seq_start = sst.get_seq_start();
    int min = sst.get_min(), max = sst.get_max();
    // std::cout << "file path" << file_path<<"\n";
    for (const auto& e: sst.getEntries()){
        // std::cout << "insert " << e.key << "\n";
        entries.push_back(e);
    }
    for (const auto& e: sst.getRangeTomb()){
        // std::cout << "inser range tombs"<< e.seq <<"\n";
        tombs.push_back(e);
    } 

    for (int i = sstables_file.at(level).size() - 2; i >= 0; i--){
        std::string path = path_control(level, sstables_file.at(level).at(i));
        // std::cout << "file path" << path<<"\n";
        SSTable new_sst(path);
        min = std::min(new_sst.get_min(), min);
        max = std::max(new_sst.get_max(), max);
        new_seq_start = std::min(new_seq_start, new_sst.get_seq_start());
        for (const auto& e: new_sst.getEntries()){
            // std::cout << "insert " << e.key << "\n";
            entries.push_back(e);
        }
        for (const auto& e: new_sst.getRangeTomb()){
            // std::cout << "inser range tombs"<< e.seq <<"\n";
            tombs.push_back(e);
        } 
    }
    // std::cout << "tomb size"<< tombs.size()<< "\n";
    // std::cout << "entries size"<< tombs.size()<< "\n";
    // std::cout << "sstables_file size"<< sstables_file.size()<< "\n";
    // std::cout << "sstables_file size"<< sstables_file.at(level).size()<< "\n";
    // std::cout << "sstables_file 0 0:"<< sstables_file.at(level).at(0)<< "\n";
    // std::cout << "sstables_file 0 1:"<< sstables_file.at(level).at(1)<< "\n";
    std::sort(entries.begin(), entries.end(), entry_cmp);
    std::sort(tombs.begin(), tombs.end(), tomb_cmp);
    MemTable svae_mmt(entries, tombs, min, max, levels_size.at(level), new_seq_start);

    for (int file_num : sstables_file.at(level)) {
        std::string old_path = path_control(level, file_num);
        std::remove(old_path.c_str());
    }
    levels_size.at(level) = 0;
    sstables_file.at(level).clear();
    int new_level = level + 1;
    if (new_level <= max_level){
        levels_size.at(new_level) += this_level_size;
        svae_mmt.save(path_control(new_level, sstables_file.at(new_level).size()));
        sstables_file.at(new_level).push_back(sstables_file.at(new_level).size());
        if (levels_size.at(new_level) >= level_size_base * pow(level_size_multi, new_level)){
            compact(new_level);
        }
    } else {
        levels_size.push_back(this_level_size);
        svae_mmt.save(path_control(new_level, 0));
        sstables_file.push_back(std::vector{0});
        max_level += 1;
    }
}

std::string templatedb::DB::path_control(int level, int num)
{
    return basic_path + std::to_string(level) + "_" + std::to_string(num) + ".data";
}

void templatedb::DB::set_flush(int num){
    flush_base = num;
}

void templatedb::DB::set_level_size(int num){
    level_size_base = num;
}

void templatedb::DB::set_level_size_multi(int num){
    level_size_multi = num;
}