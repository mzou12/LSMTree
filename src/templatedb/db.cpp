#include "templatedb/db.hpp"
#include <cmath>

using namespace templatedb;


Value DB::get(int key)
{
    Value memory_get = sstable.get(key);
    if (memory_get.visible == false){
        return Value(false);
    } else if (!memory_get.items.empty()){
        return memory_get;
    }
    for (int i = 0; i < level; i++){
        for (int j = sstables_file.at(i).size()-1; j >= 0; j--){
            int file_id = sstables_file[i][j];
            std::string path = basic_path + std::to_string(i) + "_" + std::to_string(file_id) + ".data";
            Value check = SSTable(path).get(key);
            if (check.visible && check.items.empty()){
                continue;
            }
            if (check.visible != false){
                return check;
            } else if (check.visible == false){
                return Value(false);
            }
        }
    }
    return Value(false);
}


void DB::put(int key, Value val)
{
    sstable.add(key, val);
    count+=1;
    db_size += 1;
    compact_check();
}


std::vector<Value> DB::scan() {

    // Step 3: Filter and return visible values
    std::vector<Value> result;

    return result;
}

std::vector<Value> DB::scan(int min_key, int max_key) {
    std::vector<Value> result;

    return result;
}



void DB::del(int key)
{
    sstable.point_delete(key);
    count += 1;
    db_size += 1;
    compact_check();
}


void DB::del(int min_key, int max_key)
{
    sstable.range_delete(min_key, max_key);
    count += 1;
    db_size += 1;
    compact_check();
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

bool templatedb::DB::compact_check()
{
    if (count >= level_size){
        compact(sstable);
    }
    count = 0;
    return false;
}

void templatedb::DB::compact(SSTable& new_sst) {
    std::list<SSTable> merged_list;
    merged_list.push_back(new_sst);
    int total_entries = new_sst.size;

    int target_level = -1;

    for (int i = 0; i <= level; ++i) {
        int capacity = std::pow(level_size_base, i + 1) * level_size;
        total_entries += sstable_level_size[i];

        if (total_entries > capacity) {
            for (auto it = sstables_file[i].rbegin(); it != sstables_file[i].rend(); ++it) {
                std::string path = basic_path + std::to_string(i) + "_" + std::to_string(*it) + ".data";
                SSTable sst(path);
                merged_list.push_back(sst);
            }

            sstables_file[i].clear();
            sstable_level_size[i] = 0;
        } else {
            target_level = i;
            break;
        }
    }

    if (target_level == -1) {
        target_level = ++level;
        sstables_file.push_back({});
        sstable_level_size.push_back(0);
    }

    std::list<Entry> all_entries;
    for (SSTable& sst : merged_list) {
        for (const Entry& e : sst.getEntries()) {
            all_entries.push_back(e);
        }
    }

    std::list<Entry> buffer;
    int file_id = sstables_file[target_level].empty() ? 0 : (sstables_file[target_level].back() + 1);
    for (const Entry& e : all_entries) {
        buffer.push_back(e);
        if (buffer.size() >= level_size) {
            SSTable out;
            for (const Entry& ent : buffer) out.add(ent.key,ent.val);

            std::string filename = basic_path + std::to_string(target_level) + "_" + std::to_string(file_id) + ".data";
            out.save(filename);

            sstables_file[target_level].push_back(file_id);
            sstable_level_size[target_level] += out.size;
            ++file_id;
            buffer.clear();
        }
    }

    if (!buffer.empty()) {
        SSTable out;
        for (const Entry& ent : buffer) out.add(ent.key,ent.val);

        std::string filename = basic_path + std::to_string(target_level) + "_" + std::to_string(file_id) + ".data";
        out.save(filename);

        sstables_file[target_level].push_back(file_id);
        sstable_level_size[target_level] += out.size;
    }
}
