# CS 561: Implementation LSM-Tree Key-Value Store with Different Compaction Strategies


## About

This project implements a **Log-Structured Merge-Tree (LSM-tree)** key-value store in C++. It supports:

- **Leveling and Tiering Compaction**(In separate branch)
- **Point and Range Deletion (Tombstone-based)**
- **Bloom Filters for fast GET** (In separate branch)
- **Optional Skiplist-based MemTable**(In separate branch)
- **Basic benchmarking suite**(In src/templatedb/experience.cpp)

Master branch is using tiering strategy, and we also implemented multiple compaction strategy in multiple branches


## Features

### Compaction Strategies
- **Leveling**: Merge overlapping files between levels.
- **Tiering**: No merging; accumulate files.

### Deletes
- **Point Delete**: Marks a single key as deleted then put it in to the entry list.
- **Range Delete**: Insert a range in to range tombs list.

### Optimization
- **Bloom Filters**: Avoid unnecessary disk reads on negative GETs.
- **Skiplist MemTable**: Boost PUT and GET performance in memory (Before flush).
- **Lazy Startt** Only read Header to determine read is necussary or not

### SCAN Support
- Full scan or `SCAN(min, max)` with deduplication and tombstone filtering.

## Structure

```
  project/
  |-- src/templatedb/
  │ |-- data/ .wl # workload file, store in the data folder
  │ |-- db.* # Core logic (put, get, scan, compaction)
  │ |-- MemTable.* # Memory, In-memory structure (array/skiplist), handle insert, delete
  │ |-- SSTable.* # SSTable On-disk table read
  │ |-- struct.hpp # Struct use in LSM tree, include Entry, RangeTomb, Value struct
  │ |-- BloomFilter.* # Bloom filter utility
  │ |-- murmurhash.* # Implementation of a MurmurHash hash for Bloom filter
  │ |-- operation.* # Read the workload file and allow users to input through the file
  │ |-- SkipList.hpp # Key-value type SkipList
  │ |-- experience.cpp # use for experiment
  │ |-- Makefile # Help compile experience class
  |-- tools/ # Python workload generator
  | |-- out/ default out folder
  │ |-- gen_workload.py # generate mixed workload
  │ |-- gen_sequential_insert.py # generate sequential insert
  │ |-- gen_GET.py #generate GET workload
  │ |-- gen_SCAN.py #generate SCAN workload
  │ |-- gen_sequentialPUT.py #generate sequential PUT 
  │ |-- gen_randomPUT.py #generate random PUT
  │ |-- gen_pointdelete.py #generate point delete workload
  │ |-- gen_rangedelete.py #generate range delete workload
  │ |-- gen_mixed_workload.py #generate mixed workload
  |-- format.txt # SStable files written to the disk
  |-- README.md # You are in here
```

## Getting Start
### Requirements
- C++17 compiler (e.g. g++)
- Python3 for workload generation
- GNU Make

### Compile

Generate workload:

in tools folder, use:

```bash
cd tools
#Generate mixed workload
python3 gen_workload.py <num_ops> <type> <output_path> # e.g. python3 gen_workload.py 50000 mixed ./data

or
# Generate get operation workload
python3 gen_GET.py <num_ops> <key_max> <output_folder> #e.g. python3 gen_GET.py 10000 100000 /data

or
# Generate scan operation workload
python3 gen_SCAN.py <num_ops> <scan_range> <key_max> <output_folder> #e.g. python3 gen_SCAN.py 1000 10 100000 /data

or
# Generate sequential put operation workload
python3 gen_sequentialPUT.py <num_ops> <dims> <key_max> <output_folder>  #e.g. python3 gen_sequentialPUT.py 100000 2 100000 /data

or
# Generate random put operattion workload
python3 gen_randomPUT.py <num_ops> <dims> <key_max> <output_folder> #e.g. python3 gen_randomPUT.py 100000 2 100000 /data

or
# Generate Point delete operation workload
python3 gen_pointdelete.py <num_ops> <key_max> <output_folder> #e.g. python3 gen_pointdelete.py 1000 100000 /data

or
# Generate Rondom delete opertion workload
python3 gen_rangedelete.py <num_ops> <key_max> <range_size> <output_folder> #e.g. python3 gen_rangedelete.py 1000 100000 10 /data

or
# Generate mixed operation workload
python3 gen_mixed_workload.py <total_ops> <put_ratio> <get_ratio> <point_del_ratio> <range_del_ratio> <scan_ratio> <key_max> <range_len> <output_folder> #e.g. gen_mixed_workload.py 10000 0.5 0.2 0.1 0.1 0.1 100000 10 /data

mv [file you generate] [../src/templatedb/data]

cd ../src/templatedb

# Change operation file name in the experience.cpp

make

./run_experience
```

## Key Design Choice

- Point and range tombstones are retained and filtered during both query and compaction.

- Fragment-based range deletion modeled after RocksDB.

- Multi-way heap merge used for compaction & scan.

- Bloom Filters persisted and lazily loaded from SSTable.

## Configuration

- User can set LSM tree by using db's set_flush, set_level_size, set_level_size_multi method
to set LSM tree's flush trigger, compaction trigger, level size increasement

- All SSTables saved under SSTables/.

## References

- [Rocksdb](https://github.com/facebook/rocksdb)
- Luo, C., & Carey, M. J. (2020). LSM-based storage techniques: a survey. The VLDB Journal, 29(1), 393-418.
- [BU CS561 templatedb](https://github.com/BU-DiSC/cs561_templatedb)

## Contact
If you have any questions please feel free to email to Minghong Zou zmh1225@bu.edu
