#!/bin/python
import csv
import random
import sys
import os


MAX_VALUE = 2**16 - 1

def gen_random_puts(n, dims, key_max):
    ops = []
    for _ in range(n):
        key = random.randint(0, key_max - 1)
        values = random.sample(range(MAX_VALUE), dims)
        ops.append(['I', key] + values)
    return ops

def gen_scan_ops(n, key_max, range_size):
    ops = []
    for _ in range(n):
        start = random.randint(0, key_max - range_size)
        end = start + range_size
        ops.append(['S', start, end])
    return ops

def main():
    num_puts = 100000
    num_scans = 1000
    dims = 2
    key_max = 100000
    scan_range = 1000
    folder = '/Users/harlem/Desktop/LSMTree-leveling/data'

    os.makedirs(folder, exist_ok=True)
    fname = f"{folder}/put{num_puts}_scan{num_scans}_range{scan_range}.wl"

    put_ops = gen_random_puts(num_puts, dims, key_max)
    scan_ops = gen_scan_ops(num_scans, key_max, scan_range)

    all_ops = put_ops + scan_ops
    random.shuffle(all_ops)

    with open(fname, 'w') as f:
        writer = csv.writer(f, delimiter=' ')
        writer.writerow([len(all_ops)])
        writer.writerows(all_ops)

    print(f"[✓] Workload generated: {fname}")

if __name__ == '__main__':
    main()