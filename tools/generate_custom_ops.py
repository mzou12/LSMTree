#!/bin/python
import csv
import random
import sys

MAX_VALUE = 2**16 - 1
OPERATIONS = ['I', 'Q', 'S', 'D']


def get_op(dims, key_max, op_code):
    args = [random.choice(range(key_max))]  # First arg is always a key
    if op_code == 'I':
        args += random.sample(range(MAX_VALUE), dims)
    elif op_code == 'Q':
        pass  # only key
    return [op_code] + args


def main(args):
    if ('-h' in args) or not(len(args) == 6):
        print('USAGE:\n\t%s <num_ops> <dimensions> <key_max> <folder> <op_type>' % (args[0]))
        sys.exit(0)

    num_ops = int(args[1])
    dims = int(args[2])
    key_max = int(args[3])
    folder = args[4]
    op_code = args[5]
    fname = '%s/%s_only_%s_%s_%s.wl' % (folder, op_code, num_ops, dims, key_max)

    with open(fname, 'w') as fid:
        writer = csv.writer(fid, delimiter=' ')
        writer.writerow([num_ops])
        for _ in range(num_ops):
            writer.writerow(get_op(dims, key_max, op_code))



if __name__ == '__main__':
    main(sys.argv)
