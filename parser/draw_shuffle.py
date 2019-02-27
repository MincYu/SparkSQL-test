import glob
import re
import click
import math
import numpy as np
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import statsmodels.api as sm
from os.path import isdir
from os import mkdir
import os
from matplotlib import rc
rc('mathtext', default='regular')

def extract_info(path):
    group = path.split('/')[-1]
    _mkdir('throuput/{}'.format(group))

    files_path = glob.glob('{}/*'.format(path))

    for f in files_path:
        print(f)
        name = f.split('/')[-1]
        # files_path = glob.glob(f'{path}/{name}/workerLoad*')
        print('First shuffle')
        files_path = glob.glob('{}/shuffle/workerLoad*'.format(f))
        records = get_shuffle(files_path)
        amount_list = [ int(v/(1024 * 1024)) for v in records.values()]
        # print(np.argmax(amount_list))

        fig = plt.figure()
        ax = fig.add_subplot(111)

        first_time = list(records.keys())[0]
        indexes = [  r - first_time + 1 for r in records.keys()]
        ax.plot(indexes, amount_list, '-')
        
        ax.grid()

        ax.set_xlabel("Second")
        ax.set_ylabel("Amount of shuffled data")

        plt.savefig('throuput/{}/{}.pdf'.format(group, name))

def get_shuffle(files_path):
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
                    records[timestamp] = (records[timestamp] + amount) if timestamp in records else amount

    if records:
        amount_list = [ v/(1024 * 1024) for v in records.values()]
        max_rate = max(amount_list)
        total_shuffling = sum(amount_list)
        # records.sort(key=lambda e: e[0])
        print('max_rate: {}; shuffle: {}'.format(max_rate, total_shuffling))
        return records
        
        # plt.show()
    else:
        print('No records')

            
@click.command()
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
# @click.option('--num', default=1)
def parse(path):
    dir_path = glob.glob('{}/*'.format(path))
    for d in dir_path:
        extract_info(d)

def _mkdir(newdir):
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
            _mkdir(head)
        if tail:
            os.mkdir(newdir)


if __name__ == '__main__':
    parse()
