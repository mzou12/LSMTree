#!/bin/python
import csv
import random
import sys
import os

def get_get_ops(n, key_max):
    ops = []
    for _ in range(n):
        key = random.randint(0, key_max - 1)
        ops.append(['Q', key])
    return ops

def main(args):
    if ('-h' in args) or not(len(args) == 4):
        print('USAGE:\n\t%s <num_ops> <key_max> <folder>' % (args[0]))
        sys.exit(0)

    num_ops = int(args[1])
    key_max = int(args[2])
    folder = args[3]

    os.makedirs(folder, exist_ok=True)
    fname = '%s/get_%s.wl' % (folder, num_ops)

    with open(fname, 'w') as fid:
        writer = csv.writer(fid, delimiter=' ')
        writer.writerow([num_ops])
        ops = get_get_ops(num_ops, key_max)
        writer.writerows(ops)

    print(f'Generated GET workload at: {fname}')

if __name__ == '__main__':
    main(sys.argv)