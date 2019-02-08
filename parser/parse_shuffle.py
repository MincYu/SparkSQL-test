import glob
import re
import click
import math

def extract_info(path):
    name = path.split('/')[-1]
    # files_path = glob.glob(f'{path}/{name}/workerLoad*')
    print('First shuffle')
    files_path = glob.glob('{}/shuffle/workerLoad*'.format(path))
    get_shuffle(files_path)

    print('Warm up shuffle')
    files_path = glob.glob('{}/noshuffle/workerLoad*'.format(path))
    get_shuffle(files_path)

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
    else:
        print('No records')

def extract_lateny(path):
    name = path.split('/')[-1].split('_')[0]

    files_path = glob.glob('{}/*/scale*'.format(path))

    content_regex = re.compile(r'(?P<date>\S+) (?P<time>\S+) (?P<level>\S+) Join: End executing query\. Time: (?P<times>\S+)')

    records = []
    for p in files_path:
        with open(p, 'r') as f:
            for line in f:

                match = content_regex.match(line)
                if match is None:
                    continue
                groups = match.groupdict()
                typ = p.split('/')[-2]
                records.append((typ, float(groups['times'])))

    warm_up_files = glob.glob('{}/noshuffle/warm*'.format(path))
    for p in warm_up_files:
        with open(p, 'r') as f:
            for line in f:
                match = content_regex.match(line)
                if match is None:
                    continue
                groups = match.groupdict()
                typ = p.split('/')[-1].split('.')[0]
                records.append((typ, float(groups['times'])))

    records.sort(key=lambda e: e[1], reverse=True)
    print('latency: {}'.format(records))

    gap = lambda f, s: (f - s)/s
    print('gap: {}'.format(gap(records[0][1], records[-1][1])))


@click.command()
@click.argument('path', type=click.Path(exists=True, resolve_path=True))
# @click.option('--num', default=1)
def parse(path):
    dir_path = glob.glob('{}/*'.format(path))
    for d in dir_path:
        print(d)
        extract_info(d)
        extract_lateny(d)

if __name__ == '__main__':
    parse()
