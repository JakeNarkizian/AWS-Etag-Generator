import argparse
import hashlib
import os

part_size = 2**20 * 50

def getEtag(localFilePath):
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
    args = parser.parse_args()
    print getEtag(args.path)