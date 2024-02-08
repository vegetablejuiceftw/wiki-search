import gzip
import json
import os
from itertools import islice, chain
from multiprocessing import Pool, cpu_count
import glob

import msgpack
from tqdm.auto import tqdm

from diskstorage import DiskSearch
from utils import reset_working_directory

reset_working_directory()


def reader(file_path):
    file_size = os.path.getsize(file_path)

    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=file_path, mininterval=1.5, position=0)

    old = 0
    with gzip.open(file_path, 'rb') as file:
        for line in islice(msgpack.Unpacker(file, use_list=False, raw=False), None):
            new_start_pos = file.fileobj.tell()
            pbar.update(new_start_pos - old)
            old = new_start_pos
            yield line


def gather_aliases(shard_name):
    alias_mapping = {}
    for line in reader(shard_name):
        key = line['id']
        aliases = set(
            label
            for label in chain(
                (label for lang, label in line['labels']),
                # (label for lang, label in line['descriptions']),
                (label for lang, label in line['aliases']),
                (label for lang, label, _ in line['sitelinks']),
            )
        )
        for lookup in aliases:
            ids = alias_mapping.get(lookup)
            if ids is not None:
                ids.add(key)
            else:
                alias_mapping[lookup] = {key}
    return alias_mapping


def gather_values(shard_name):
    out = []
    for line in reader(shard_name):
        aliases = set(
            label
            for label in chain(
                (label for lang, label in line['labels']),
                (label for lang, label in line['aliases']),
                (label for lang, label, _ in line['sitelinks']),
            )
        )
        aliases = sorted(aliases)
        text = next((label for lang, label in line['descriptions'] if lang == 'en'), None)
        if text is None:
            text = next((label for lang, label in line['descriptions'] if lang.startswith('en')), None)
        out.append(
            (line['id'],  {
                'id': line['id'],
                'text': text,
                'aliases': aliases,
            })
        )
    return out


SHARD_PATH = "/home/derf/projects/wikidata-parsing/data/shard-16-*.gz"
shards = list(glob.glob(SHARD_PATH))
assert shards, f"are we sure that the `SHARD_PATH = {SHARD_PATH}` is correct?"


dataset_aliases = DiskSearch('data/wikidata.cache')
with Pool(len(shards)) as p:
    for mapping in tqdm(p.imap(gather_values, shards), desc='collecting', total=len(shards)):
        dataset_aliases.write(mapping)


# alias_mapping = {}
# with Pool(len(shards)) as p:
#     for mapping in tqdm(p.imap(gather_aliases, shards), desc='collecting', total=len(shards)):
#         for k, v in mapping.items():
#             alies = alias_mapping.get(k)
#             if alies is not None:
#                 alies.update(v)
#             else:
#                 alias_mapping[k] = v
#
# dataset_aliases = DiskSearch('data/wikidata.aliases2.cache')
# dataset_aliases.write(tqdm(
#     (
#         (k, tuple(sorted(v, key=lambda e: (len(e), e))))
#         for k, v in alias_mapping.items()
#     ),
#     total=len(alias_mapping), desc="Writer"))
#
# print(dataset_aliases['Donald Trump'])
# print(dataset_aliases['Estonia'])
