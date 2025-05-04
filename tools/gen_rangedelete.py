#!/bin/python
import csv
import random
import sys
import os

def get_range_delete_ops(n, key_max, range_size):
    ops = []
    for _ in range(n):
        start = random.randint(0, key_max - range_size)
        end = start + range_size
        ops.append(['D', start, end])
    return ops

def main(args):
    if ('-h' in args) or not(len(args) == 5):
        print('USAGE:\n\t%s <num_ops> <key_max> <range_size> <folder>' % (args[0]))
        sys.exit(0)

    num_ops = int(args[1])
    key_max = int(args[2])
    range_size = int(args[3])
    folder = args[4]

    os.makedirs(folder, exist_ok=True)
    fname = '%s/range_delete_%s_range%d.wl' % (folder, num_ops, range_size)

    with open(fname, 'w') as fid:
        writer = csv.writer(fid, delimiter=' ')
        writer.writerow([num_ops])
        ops = get_range_delete_ops(num_ops, key_max, range_size)
        writer.writerows(ops)

    print(f'Generated range DELETE workload at: {fname}')

if __name__ == '__main__':
    main(sys.argv)