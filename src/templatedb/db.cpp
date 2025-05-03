#include "db.hpp"
#include <cmath>
#include <set>
#include <algorithm>
#include <unordered_set>
#include <queue>
#include <functional>
#include <map>
#include <iomanip>

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
            std::string path = path_control(sstables_file.at(i).at(j));
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
    if (a.start != b.start) return a.start < b.start; 
    return a.seq > b.seq;
}


static std::vector<Fragment> build_fragments(const std::vector<RangeTomb>& tombs) {
    // 1. collect bond
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
            left = mid + 1;   // key in the right
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

    // build fragemtn
    std::vector<RangeTomb> tombs;

    mmt.reset_range_iterator();
    while (mmt.range_tombs_has_next())
        tombs.push_back(mmt.range_tombs_next().value());

    std::vector<SSTable> sstables;
    for (int i = 0; i <= max_level; ++i) {
        for (int j = sstables_file[i].size() - 1; j >= 0; --j) {
            std::string path = path_control(sstables_file.at(i).at(j));
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

    //  heap merge
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
            std::string path = path_control(sstables_file.at(i).at(j));
            sstables.emplace_back(path);
            sstables.back().reset_range_iterator();
            while (sstables.back().range_tombs_has_next())
                tombs.push_back(sstables.back().range_tombs_next().value());
        }
    }

    std::vector<Fragment> fragments = build_fragments(tombs);

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

        //right bond
        if (e.key >= max_key)
            break;

        // left bond
        if (e.key < min_key) {
        
        } else if (!seen_keys.count(e.key)) {
            seen_keys.insert(e.key);

            if (!e.tomb && !is_key_covered_by_fragment(fragments, e.key, e.seq)) {
                result.push_back(e.val);
            }
        }

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

// delete from [min,max), notice not [4,6]!!!!
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
    int file_id = generate_id();
    std::string path = path_control(file_id);
    sstables_file.at(0).push_back(file_id); // Add file's num
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

// void templatedb::DB::normalize_filenames(int level){
//     if (level >= sstables_file.size() || sstables_file[level].empty()) 
//         return;

//     for (int i = 0; i < sstables_file[level].size(); ++i){
//         if(sstables_file[level].at(i) != i){
//             std::rename(path_control(level, sstables_file[level].at(i)).c_str(),path_control(level, i).c_str());
//         }
//     }
//     int num = sstables_file[level].size();
//     sstables_file[level].clear();
//     for (int i = 0; i < num; ++i){
//         sstables_file[level].push_back(i);
//     }
// }

void templatedb::DB::compact(int level) {

    // choose oldest one in this level become cs(compacted sstable)
    int oldest_level_num = sstables_file.at(level).at(0);
    // std::cout<<"Level: "<< level <<"\n";
    SSTable cs(path_control(oldest_level_num));
    int cs_min = cs.get_min();
    int cs_max = cs.get_max();
    int cs_size = cs.get_size();

    // Hit max_level, need open a new level, so it is oldest data, do point delete and range delte cleaning
    if (level >= max_level){
        cs.reset_iterator();
        std::vector<RangeTomb> all_tombs = cs.getRangeTomb();
        std::vector<Entry> merged_entries;
        std::vector<Fragment> fragments = build_fragments(all_tombs);
        int min = INT32_MAX;
        int max = INT32_MIN;
        int start_seq = INT32_MAX;
        while(cs.has_next()){
            Entry e = cs.next().value();
            if (!e.tomb && !is_key_covered_by_fragment(fragments, e.key, e.seq)){
                min = std::min(e.key, min);
                max = std::max(e.key, max);
                start_seq = std::min((int)e.seq, start_seq);
                merged_entries.push_back(e);
            }
        }
        all_tombs.clear();
        if (merged_entries.empty()){
            auto& filelist = sstables_file[level];
            auto it = std::find(filelist.begin(), filelist.end(), oldest_level_num);
            if (it != filelist.end()) 
                filelist.erase(it);
                
            levels_size[level] -= cs.get_size();
            std::remove(path_control(oldest_level_num).c_str());
            // normalize_filenames(level);
            return;
        }
        MemTable new_mmt(merged_entries, all_tombs, min, max, merged_entries.size(), start_seq);
        int new_id = generate_id();
        new_mmt.save(path_control(new_id));

        auto& filelist = sstables_file[level];
        auto it = std::find(filelist.begin(), filelist.end(), oldest_level_num);
        if (it != filelist.end()) 
            filelist.erase(it);
        
        sstables_file.push_back({new_id});
        levels_size[level] -= cs.get_size();
        levels_size.push_back(merged_entries.size());
        std::remove(path_control(oldest_level_num).c_str());
        // normalize_filenames(level);
        max_level++;
        return;
    }

    int overlap_sum = 0;
    std::vector<SSTable> overlap_table;
    std::vector<int> overlap_files;
    int min = INT32_MAX;
    int max = INT32_MIN;
    int start_seq = INT32_MAX;
    for (int i = sstables_file[level + 1].size() - 1; i >=0 ; --i){
        int file_id = sstables_file[level+1][i];
        SSTable sst(path_control(file_id));
        if (sst.get_min() <= cs_max && sst.get_max() >= cs_min){
            overlap_table.emplace_back(std::move(sst));
            overlap_files.push_back(file_id);
            overlap_sum += sst.get_size();
        }
    }

    if (overlap_sum == 0){
        sstables_file[level + 1].push_back(oldest_level_num);
        levels_size[level + 1] += cs_size;
        levels_size[level] -= cs_size;
        auto& filelist = sstables_file[level];
        auto it = std::find(filelist.begin(), filelist.end(), oldest_level_num);
        if (it != filelist.end()) 
            filelist.erase(it);

        // normalize_filenames(level);
        if (levels_size[level + 1] >= level_size_base * pow(level_size_multi, level + 1)){
            compact(level + 1);
        }
        return;
    }

    std::vector<Entry> merged_entries;
    std::vector<RangeTomb> all_tombs = cs.getRangeTomb();
    
    for (auto& sst : overlap_table) {
        const auto& ts = sst.getRangeTomb();
        all_tombs.insert(all_tombs.end(), ts.begin(), ts.end());
    }
    cs.reset_iterator();
    for (auto& sst : overlap_table){
        sst.reset_iterator();
    } 

    struct Item {
        Entry entry;
        int source;
    };
    auto cmp = [](const Item& a, const Item& b) {
        if (a.entry.key != b.entry.key) return a.entry.key > b.entry.key;
        return a.entry.seq < b.entry.seq;
    };

    std::priority_queue<Item, std::vector<Item>, decltype(cmp)> pq(cmp);
    if (cs.has_next()) 
        pq.push({cs.next().value(), 0});
    
    for (int i = 0; i < overlap_table.size(); ++i) {
        if (overlap_table[i].has_next()) {
            pq.push({overlap_table[i].next().value(), i + 1});
        }
    }

    std::vector<Fragment> fragments = build_fragments(all_tombs);

    std::unordered_set<int> seen;
    std::vector<Entry> new_entries;
    while (!pq.empty()) {
        Item item = pq.top(); pq.pop();
        Entry e = item.entry;
        int src = item.source;
    
        std::vector<Item> same_key_items;
        same_key_items.push_back(item);
    
        while (!pq.empty() && pq.top().entry.key == e.key) {
            same_key_items.push_back(pq.top());
            pq.pop();
        }
    
        Item newest = *std::max_element(same_key_items.begin(), same_key_items.end(), 
            [](const Item& a, const Item& b) {
                return a.entry.seq < b.entry.seq;
            });
    
        if (!is_key_covered_by_fragment(fragments, newest.entry.key, newest.entry.seq)) {
            start_seq = std::min((int)newest.entry.seq, start_seq);
            min = std::min(newest.entry.key, min);
            max = std::max(newest.entry.key, max);
            new_entries.push_back(newest.entry);
        }
    
        for (const Item& it : same_key_items) {
            if (it.source == 0) {
                if (cs.has_next()) pq.push({cs.next().value(), 0});
            } else {
                if (overlap_table[it.source - 1].has_next())
                    pq.push({overlap_table[it.source - 1].next().value(), it.source});
            }
        }
    }
    std::sort(all_tombs.begin(), all_tombs.end(), tomb_cmp);
    std::map<std::pair<int, int>, RangeTomb> tomb_map;
    for (auto& t : all_tombs) {
        auto key = std::make_pair(t.start, t.end);
        if (!tomb_map.count(key) || tomb_map[key].seq < t.seq) {
            start_seq = std::min((int)t.seq, start_seq);
            min = std::min(t.start, min);
            max = std::max(t.end, max);
            tomb_map[key] = t;
        }
    }
    std::vector<RangeTomb> deduped_tombs;
    for (auto& [k, v] : tomb_map) {
        deduped_tombs.push_back(v);
    }
    std::sort(deduped_tombs.begin(), deduped_tombs.end(), tomb_cmp); // sort it, maybe can delete? not sure
    int new_file_id = generate_id();
    MemTable new_sstable =  MemTable(new_entries, deduped_tombs, min, max, new_entries.size()+deduped_tombs.size(), start_seq);
    new_sstable.save(path_control(new_file_id));
    sstables_file[level + 1].push_back(new_file_id);
    levels_size[level + 1] += (new_entries.size()+deduped_tombs.size() - overlap_sum);
    db_size -= overlap_sum + cs_size - (new_entries.size()+deduped_tombs.size() );

    std::remove(path_control(oldest_level_num).c_str());
    auto& filelist = sstables_file[level];
    auto it = std::find(filelist.begin(), filelist.end(), oldest_level_num);
    if (it != filelist.end()) 
        filelist.erase(it);

    levels_size[level] -= cs_size;

    for (int f : overlap_files) {;
        std::remove(path_control(f).c_str());
    }

    std::vector<int> updated_filelist;
    for (int f : sstables_file[level + 1]) {
        if (std::find(overlap_files.begin(), overlap_files.end(), f) == overlap_files.end()) {
            updated_filelist.push_back(f);
        }
    }
    sstables_file[level + 1] = updated_filelist;

    // normalize_filenames(level);
    // normalize_filenames(level + 1);
    if (levels_size[level + 1] >= level_size_base * pow(level_size_multi, level + 1)){
        // std::cout << "Start recursion"<<"\n";
        
        compact(level + 1);
    }

}


std::string templatedb::DB::path_control(int file_id)
{
    std::ostringstream oss;
    oss << basic_path << std::setw(6) << std::setfill('0') << file_id << ".data";
    return oss.str();

}

int templatedb::DB::generate_id(){
    return unique_file_id++;
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