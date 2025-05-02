#ifndef _TEMPLATEDB_DB_H_
#define _TEMPLATEDB_DB_H_

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "operation.hpp"
#include "SSTable.hpp"
#include "MemTable.hpp"
#include "struct.hpp"

namespace templatedb
{

typedef enum _status_code
{
    OPEN = 0,
    CLOSED = 1,
    ERROR_OPEN = 100,
} db_status;

class DB
{
public:
    db_status status;

    DB();
    ~DB() {close();};

    Value get(int key);
    void put(int key, Value val);
    std::vector<Value> scan();
    std::vector<Value> scan(int min_key, int max_key);
    void del(int key);
    void del(int min_key, int max_key);
    size_t size();

    db_status open(std::string & fname);
    bool close();

    bool load_data_file(std::string & fname);
    void flush();

    std::vector<Value> execute_op(Operation op);

    void set_flush(int num);
    void set_level_size(int num);
    void set_level_size_multi(int num);

private:
    std::fstream file;
    std::unordered_map<int, Value> table;
    MemTable mmt;
    size_t value_dimensions = 0;
    
    bool write_to_file();
    bool flush_check();
    void compact(int level);
    std::string path_control(int file_id);
    int max_level = 0;
    int count = 0;
    int db_size = 0;
    uint64_t seq = 0;
    std::vector<std::vector<int>> sstables_file; // sstable file name
    std::vector<int> levels_size; //each level's current size
    int flush_base = 10;
    int level_size_base = 20; // Basic level size
    int level_size_multi = 4;
    const std::string basic_path = "SSTables/SSTable_";
    // void normalize_filenames(int level);
    int generate_id();
    int unique_file_id = 0;
};

}   // namespace templatedb

#endif /* _TEMPLATEDB_DB_H_ */