# CS 561: Implementation LSM-Tree Key-Value Store with Different Compaction Strategies


## About

This project implements a **Log-Structured Merge-Tree (LSM-tree)** key-value store in C++. It supports:

- **Leveling and Tiering Compaction**(In separate branch)
- **Point and Range Deletion (Tombstone-based)**
- **Bloom Filters for fast GET** (In separate branch)
- **Optional Skiplist-based MemTable**(In separate branch)
- **Basic benchmarking suite**


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
  │ |-- experience.cpp # use for experience
  │ |-- Makefile # Help compile experience class
  |-- tools/ # Python workload generator
  | |-- out/ default out folder
  │ |-- gen_workload.py # generate mixed workload
  │ |-- gen_sequential_insert.py # generate sequential insert
  │ |--
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
python3 gen_workload.py <num_ops> <type> <output_path> # e.g. python3 gen_workload.py 50000 mixed ./out/mixed.workload

cd src/templatedb
make
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