import glob
import re
import math
from os.path import isdir
from os import mkdir
import os

# content_regex = re.compile(r'(?P<date>\S+) (?P<time>\S+) (?P<level>\S+) Join: End executing query\. Time: (?P<times>\S+)')
content_regex = re.compile(r'(?P<date>\S+) (?P<time>\S+) (?P<level>\S+) TpchQuery: End executing query\. Time: (?P<times>\S+)')

def get_shuffle_with_file(files_path):
    records = {}
    for f in files_path:
        name = f.split('/')[-1].split('_')[1].split('.')[0]
        s = list(get_shuffle_with_id([f]).values())[0]
        records[name] = s
    return records

def get_shuffle_with_id(files_path):
    records = {}
    for p in files_path:
        with open(p) as f:
            for line in f:
                content = line.split('\t')
                # second
                timestamp = int(int(content[0])/1000)
                # Bytes
                if len(content) >= 3:
                    amount = int(content[2])
                    process_id = -1 if len(content) == 3 else int(content[3].split('@')[0])
                    if process_id in records:
                        records[process_id][timestamp] = (records[process_id][timestamp] + amount) if timestamp in records[process_id] else amount
                    else:
                        records[process_id] = {timestamp : amount}

    if records:
        amount_list = {}
        [ amount_list.update({k: [ i/(1024 * 1024) for i in v.values() ]}) for k, v in records.items()]

        # max_rate = get_most_fre([ int(i * 8 * 1024 * 1024 / 1000000) for i in amount_list])
        for p, a in amount_list.items():
            total_shuffling = sum(a)
            print(f'id: {p}; amount: {total_shuffling}; len: {len(a)}')

        return amount_list
    else:
        print('No records')

def get_latency_with_id(files_path):

    records = {}
    for p in files_path:
        name = p.split('/')[-1].split('.')[0]
        with open(p, 'r') as f:
            for line in f:

                match = content_regex.match(line)
                if match is None:
                    continue
                groups = match.groupdict()
                records[name] = float(groups['times'])
    return records

gap = lambda f, s: round((f - s)/s, 4)

def mkdir(newdir):
    """
    works the way a good mkdir should :)
        - already exists, silently complete
        - regular file in the way, raise an exception
        - parent directory(ies) does not exist, make them as well
    """
    if type(newdir) is not str:
        newdir = str(newdir)
    if os.path.isdir(newdir):
        pass
    elif os.path.isfile(newdir):
        raise OSError("a file with the same name as the desired " \
                      "dir, '%s', already exists." % newdir)
    else:
        head, tail = os.path.split(newdir)
        if head and not os.path.isdir(head):
            mkdir(head)
        if tail:
            os.mkdir(newdir)
