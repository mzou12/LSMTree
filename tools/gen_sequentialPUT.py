#!/bin/python
import csv
import random
import sys
import os

MAX_VALUE = 2**16 - 1

def get_sequential_insert_ops(dims, num_ops):
    ops = []
    for key in range(num_ops):
        values = random.sample(range(MAX_VALUE), dims)
        ops.append(['I', key] + values)
    return ops

def main(args):
    if ('-h' in args) or not(len(args) == 5):
        print('USAGE:\n\t%s <num_ops> <dimensions> <key_max> <folder>' % (args[0]))
        sys.exit(0)

    num_ops = int(args[1])
    dims = int(args[2])
    key_max = int(args[3])
    folder = args[4]

    os.makedirs(folder, exist_ok=True)
    fname = '%s/sequential_puts_%s_%s.wl' % (folder, num_ops, key_max)

    with open(fname, 'w') as fid:
        writer = csv.writer(fid, delimiter=' ')
        writer.writerow([num_ops])
        ops = get_sequential_insert_ops(dims, num_ops)
        writer.writerows(ops)

    print(f'Generated sequential PUT workload at: {fname}')

if __name__ == '__main__':
    main(sys.argv)