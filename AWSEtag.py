import argparse
import hashlib
import os

def getEtag(localFilePath, part_size):
    with open(localFilePath, 'r') as f:
        curr = 0
        hash_list = []
        while curr < os.path.getsize(localFilePath):
            hash_list.append(hashlib.md5(f.read(part_size)).digest())
            curr += part_size

        return "%s-%d" % (hashlib.md5(''.join(hash_list)).hexdigest(), len(hash_list))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('part_size', type=int, nargs='?', default=(2**20 * 50))
    args = parser.parse_args()
    print getEtag(args.path, args.part_size)