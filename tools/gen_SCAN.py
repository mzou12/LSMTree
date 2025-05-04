#!/bin/python
import csv
import random
import sys
import os

def get_scan_ops(n, key_max, scan_range):
    ops = []
    for _ in range(n):
        start = random.randint(0, key_max - scan_range)
        end = start + scan_range
        ops.append(['S', start, end])
    return ops

def main(args):
    if ('-h' in args) or not(len(args) == 5):
        print('USAGE:\n\t%s <num_ops> <scan_range> <key_max> <folder>' % (args[0]))
        sys.exit(0)

    num_ops = int(args[1])
    scan_range = int(args[2])
    key_max = int(args[3])
    folder = args[4]

    os.makedirs(folder, exist_ok=True)
    fname = '%s/scan_%s_range%d.wl' % (folder, num_ops, scan_range)

    with open(fname, 'w') as fid:
        writer = csv.writer(fid, delimiter=' ')
        writer.writerow([num_ops])
        ops = get_scan_ops(num_ops, key_max, scan_range)
        writer.writerows(ops)

    print(f'Generated SCAN workload at: {fname}')

if __name__ == '__main__':
    main(sys.argv)