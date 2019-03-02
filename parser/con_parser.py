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
from utils import *


@click.command()
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def parse_all_latency(path):
    folder_path = glob.glob('{}/*'.format(path))
    all_shuffle = []
    all_noshuffle = []

    folder_path.sort()
    for f in folder_path:
        name = f.split('/')[-1]

        files_path = glob.glob('{}/shuffle/scale*'.format(f))
        shuffle_latency = get_latency_with_id(files_path)

        files_path = glob.glob('{}/noshuffle/scale*'.format(f))
        noshuffle_latency = get_latency_with_id(files_path)

        all_shuffle.append([ v/1000 for v in shuffle_latency.values()])
        all_noshuffle.append([ v/1000 for v in noshuffle_latency.values()])

    draw_error_bars(all_shuffle, all_noshuffle)

def draw_error_bars(shuffling, noshuffling):
    index =[]
    x = 1
    for _ in np.arange(len(shuffling)):
        index.append(x)
        x *= 2

    index = np.array(index)
    bar_width = 0.3

    ys = [sum(s)/len(s) for s in shuffling] 
    yn = [sum(n)/len(n) for n in noshuffling] 

    es = [np.std(s) for s in shuffling]
    en = [np.std(n) for n in noshuffling]

    plt.errorbar(index - bar_width, ys, es, linestyle='--', fmt='-', capsize=5, capthick=1, label='Shuffling')
    plt.errorbar(index + bar_width, yn, en, linestyle='--', fmt='-', capsize=5, capthick=1, label='Non-shuffling')

    plt.xticks(index)
    plt.legend(loc=2)

    plt.xlabel('concurrency')
    plt.ylabel('latency (seconds)')

    plt.savefig('fig/con_latency_0.pdf')
    # plt.show()

@click.command()
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def parse_all_footprint(path):
    folder_path = glob.glob('{}/*'.format(path))

    for f in folder_path:
        name = f.split('/')[-1]
        print(name)
        files_path = glob.glob('{}/shuffle/workerLoad*'.format(f))
        shuffle_data = get_shuffle_with_id(files_path)

        files_path = glob.glob('{}/shuffle/scale*'.format(f))
        shuffle_l = get_latency_with_id(files_path)

        files_path = glob.glob('{}/noshuffle/scale*'.format(f))
        noshuffle_l = get_latency_with_id(files_path)

        print(f'shuffle: {shuffle_l}; noshuffle: {noshuffle_l}; gap: {gap(list(shuffle_l.values())[0], list(noshuffle_l.values())[0])}')

        draw_footprint(shuffle_data, f'footprintjvm/{name}')

@click.command()
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
def mul_process_footprint(path):
    name = path.split('/')[-1]
    print('shuffle')
    files_path = glob.glob('{}/shuffle/workerLoad*'.format(path))
    shuffle_data = get_shuffle_with_id(files_path)

    files_path = glob.glob('{}/shuffle/scale*'.format(path))
    shuffle_l = get_latency_with_id(files_path)

    files_path = glob.glob('{}/noshuffle/scale*'.format(path))
    noshuffle_l = get_latency_with_id(files_path)

    print(f'shuffle: {shuffle_l}; nonshuffle: {noshuffle_l}')
    # draw_footprint(shuffle_data, 'fig/en_cache')

def draw_box_plot(shuffle_latency, noshuffle_latency):
    fig = plt.figure()
    bplot = plt.boxplot([shuffle_latency, noshuffle_latency],
        notch=False,
        vert=True,
        meanline=True,
        patch_artist=True)
    
    colors = ['lightblue', 'lightgreen']
    for patch, color in zip(bplot['boxes'], colors):
        patch.set_facecolor(color)

    plt.xticks([y+1 for y in range(2)], ['shuffling', 'nonshuffling'])
    plt.xlabel('measurement x')
    t = plt.title('Box plot')
    plt.show()

def draw_footprint(shuffle_data, path='fig/footprint'):
    shuffle_list = []
    labels = []
    index_num = max([len(i) for i in shuffle_data.values()]) + 1

    for i, amount in shuffle_data.items():
        whole_amount = [ int(a * 8 * 1024 * 1024 / 1000000) for a in amount]
        [whole_amount.append(0) for j in range(index_num - len(whole_amount))]

        shuffle_list.append(whole_amount)

    [labels.append(f'job {i}') for i in range(1, len(shuffle_list) + 1)]
    fig = plt.figure()
    ax = fig.add_subplot(111)

    index = range(index_num)

    ax.stackplot(index, *shuffle_list)

    y_max = max([max(i) for i in shuffle_list])
    print(y_max)

    ax.set_xlim(0, index_num)
    ax.set_ylim(0, 3500)
    ax.set_xlabel("Second")
    ax.set_ylabel("Data transfer rate (Mbps)")
    # plt.show()
    plt.savefig(f'{path}.pdf')

if __name__ == '__main__':
    parse_all_footprint()
    # parse_all_latency()
    # mul_process_footprint()
