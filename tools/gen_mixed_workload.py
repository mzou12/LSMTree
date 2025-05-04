#!/bin/python
import csv
import random
import sys
import os

MAX_VALUE = 2**16 - 1

def gen_put_ops(n, dims, key_max):
    ops = []
    for _ in range(n):
        key = random.randint(0, key_max - 1)
        values = random.sample(range(MAX_VALUE), dims)
        ops.append(['I', key] + values)
    return ops

def gen_point_delete_ops(n, key_max):
    return [['D', random.randint(0, key_max - 1)] for _ in range(n)]

def gen_range_delete_ops(n, key_max, range_size):
    ops = []
    for _ in range(n):
        start = random.randint(0, key_max - range_size)
        end = start + range_size
        ops.append(['D', start, end])
    return ops

def gen_get_ops(n, key_max):
    return [['Q', random.randint(0, key_max - 1)] for _ in range(n)]

def gen_scan_ops(n, key_max, scan_range):
    ops = []
    for _ in range(n):
        start = random.randint(0, key_max - scan_range)
        end = start + scan_range
        ops.append(['S', start, end])
    return ops

def main():
    total_ops = 50000
    dims = 2
    key_max = 100000
    range_delete_size = 10
    scan_range = 10
    folder = '/Users/harlem/Desktop/LSMTree-leveling/data'

    counts = {
        'put': int(total_ops * 0.5),     # 5000
        'pdel': int(total_ops * 0.1),    # 1000
        'rdel': int(total_ops * 0.1),    # 1000
        'get': int(total_ops * 0.2),     # 2000
        'scan': int(total_ops * 0.1),    # 1000
    }

    os.makedirs(folder, exist_ok=True)
    fname = f"{folder}/5mixed_10k_put50_pdel10_rdel10_get20_scan10.wl"

    ops = []
    ops += gen_put_ops(counts['put'], dims, key_max)
    ops += gen_point_delete_ops(counts['pdel'], key_max)
    ops += gen_range_delete_ops(counts['rdel'], key_max, range_delete_size)
    ops += gen_get_ops(counts['get'], key_max)
    ops += gen_scan_ops(counts['scan'], key_max, scan_range)

    random.shuffle(ops)

    with open(fname, 'w') as f:
        writer = csv.writer(f, delimiter=' ')
        writer.writerow([len(ops)])
        writer.writerows(ops)

    print(f"[✓] Mixed workload saved to: {fname}")

if __name__ == '__main__':
    main()