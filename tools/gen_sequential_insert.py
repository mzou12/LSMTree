#!/bin/python
import csv
import random
import sys

MAX_VALUE = 2**16 - 1

def get_insert_op(key, dims):
    args = [key]
    args += random.sample(range(MAX_VALUE), dims)
    return ['I'] + args

def main(args):
    if ('-h' in args) or not(len(args) == 5):
        print('USAGE:\n\t%s <num_ops> <dimensions> <key_start> <folder>' % (args[0]))
        sys.exit(0)

    num_ops = int(args[1])
    dims = int(args[2])
    key_start = int(args[3])
    folder = args[4]
    fname = f'{folder}/I_seq_{num_ops}_{dims}_{key_start}.wl'

    with open(fname, 'w') as fid:
        writer = csv.writer(fid, delimiter=' ')
        writer.writerow([num_ops])
        for i in range(num_ops):
            writer.writerow(get_insert_op(key_start + i, dims))

if __name__ == '__main__':
    main(sys.argv)
