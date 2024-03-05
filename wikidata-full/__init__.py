import gzip
import os, re
from functools import lru_cache
from itertools import islice, chain
from multiprocessing import Pool, cpu_count
import glob

import msgpack
from tqdm.auto import tqdm

from diskstorage import DiskSearch
from utils import reset_working_directory, normalize
from num2words import num2words

reset_working_directory()



class NumberFixer:
    number_pre = re.compile(r'hydroxy|methyl|-?tetra|-tris|beta|amino|benzene|sulfo|amido|phospho|esterase|tuluoyl|metho|alpha|zoline|oxy|phenyl|benzi|Page:|Template:', flags=re.IGNORECASE)
    number_re = re.compile(r'\b([^\s\d]*?)(\d{1,3}(?:\.\d{1,2})?)([^\s\d]*?)\b')
    number_to = {
        "th": 'ordinal',
        "nd": 'ordinal',
        "st": 'ordinal',
        "rd": 'ordinal',
        "": 'cardinal',
    }

    @classmethod
    def number_replace(cls, match: re.Match):
        prefix, number, suffix = match.group(1), match.group(2), match.group(3)
        prefix = prefix.rstrip("-/.,")
        to = cls.number_to.get(suffix, "cardinal")
        try:
            result = prefix + " " if prefix else ""
            if to == "ordinal" and "." in number:
                a,b = number.split(".")
                result = num2words(
                    a,
                    to=to
                ) + "point" + num2words(
                    b,
                    to=to
                )
            else:
                result += num2words(
                    number,
                    to=to
                )
            if suffix not in cls.number_to:
                result += " " + suffix

            return result

        except:
            print([match.group()], [prefix, number, suffix], "FAIL")
            return match.group()

    @classmethod
    @lru_cache
    def number_fix(cls, string: str):
        if cls.number_pre.findall(string):
            return [string]

        fixed = cls.number_re.sub(cls.number_replace, string)
        if fixed == string:
            return [string]
        # print(string, fixed, "", sep="\n")
        return [string, fixed]


def reader(file_path):
    file_size = os.path.getsize(file_path)

    pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc=file_path, mininterval=1.5, position=0)

    old = 0
    with gzip.open(file_path, 'rb') as file:
        for line in islice(msgpack.Unpacker(file, use_list=False, raw=False), None):
            new_start_pos = file.fileobj.tell()  # type: ignore
            pbar.update(new_start_pos - old)
            old = new_start_pos
            yield line


def get_aliases(iterator):
    aliases = {
        label: label
        for lang, label in iterator
        if lang in ("en", "enwiki", "simple", "de", "es", "nl")
    }.keys()
    aliases = (normalize(a) for a in aliases)
    aliases = (e for a in aliases for e in NumberFixer.number_fix(a))
    aliases = tuple(a for a in aliases if a)
    return aliases


def gather_aliases(shard_name):
    alias_mapping = {}
    for line in reader(shard_name):
        key = line['id']
        aliases = get_aliases(
            chain(
                ((lang, label) for lang, label in line['labels']),
                ((lang, label) for lang, label in line['aliases']),
                ((lang, label) for lang, label, _ in line['sitelinks'] if ":" not in label),
            )
        )
        for lookup in aliases:
            lookup = lookup.lower()
            ids = alias_mapping.get(lookup)
            if ids is not None:
                ids.add(key)
            else:
                alias_mapping[lookup] = {key}
    return alias_mapping


def gather_values(shard_name):
    out = []
    for line in reader(shard_name):
        aliases = get_aliases(
            chain(
                ((lang, label) for lang, label in line['labels']),
                ((lang, label) for lang, label in line['aliases']),
                ((lang, label) for lang, label, _ in line['sitelinks'] if ":" not in label),
            )
        )
        labels = get_aliases(
            (lang, label) for lang, label in line['labels']
        )
        languages = tuple({
            lang
            for lang, _ in chain(
                ((lang, label) for lang, label in line['labels']),
                ((lang, label) for lang, label in line['aliases']),
                ((lang, label) for lang, label, _ in line['sitelinks'] if ":" not in label),
            )
        })

        # aliases = sorted(aliases)
        text = next((label for lang, label in line['descriptions'] if lang == 'en'), None)
        if text is None:
            text = next((label for lang, label in line['descriptions'] if lang.startswith('en')), None)
        out.append(
            (line['id'], {
                'id': line['id'],
                'text': text,
                'labels': labels,
                'aliases': aliases,
                # 'languages': languages,
                'count_aliases': len(aliases),
                'count_languages': len(languages),
            })
        )
    return out


SHARD_PATH = "/home/derf/projects/wikidata-parsing/data/shard-16-*.gz"
shards = list(glob.glob(SHARD_PATH))
assert shards, f"are we sure that the `SHARD_PATH = {SHARD_PATH}` is correct?"

version = "v3"

dataset_full = DiskSearch(f'data/wikidata-{version}.cache')
with Pool(len(shards)) as p:
    for mapping in tqdm(p.imap(gather_values, shards), desc='collecting', total=len(shards)):
        dataset_full.write(mapping)

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
# dataset_aliases = DiskSearch(f'data/wikidata-{version}.aliases-lc.cache')
# dataset_aliases.write(tqdm(
#     (
#         (k, tuple(sorted(v, key=lambda e: (len(e), e))))
#         for k, v in alias_mapping.items()
#     ),
#     total=len(alias_mapping), desc="Writer"))
#
# print(dataset_aliases['Donald Trump'.lower()])
# print(dataset_aliases['Estonia'.lower()])
