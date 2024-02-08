from os.path import isfile

import gzip
import os
import tarfile

from tqdm import tqdm
import requests

from diskstorage import DiskSearch
from utils import reset_working_directory


def download(url, force=False):
    file_name = url.split("/")[-1].split("?")[0]
    path = f"data/{file_name}"
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    block_size = 1024

    if not isfile(path) or force:
        with tqdm(total=total_size, unit="B", unit_scale=True, desc=file_name) as progress_bar:
            with open(path, "wb") as handle:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    handle.write(data)


def iter_data(file_name, key="wikidata5m_entity.txt"):
    if file_name.endswith('.tar.gz'):
        with tarfile.open(file_name, 'r') as file:
            stream = file.extractfile(key)
            total_size, old = stream.tell(), 0
            with tqdm(total=total_size, unit="B", unit_scale=True, desc="emb", smoothing=0.1,
                      maxinterval=0.5) as progress_bar:
                for line in stream:
                    new_start_pos = stream.tell()
                    progress_bar.update(new_start_pos - old)
                    old = new_start_pos
                    yield tuple(r.strip() for r in line.decode('utf-8').split("\t"))
    else:
        with gzip.open(file_name, 'rb') as file:
            total_size, old = os.path.getsize(file_name), 0
            with tqdm(total=total_size, unit="B", unit_scale=True, desc="emb", smoothing=0.1,
                      maxinterval=0.5) as progress_bar:
                stream = file
                for line in stream:
                    new_start_pos = file.fileobj.tell()
                    progress_bar.update(new_start_pos - old)
                    old = new_start_pos
                    yield tuple(r.strip() for r in line.decode('utf-8').split("\t"))


if __name__ == '__main__':
    reset_working_directory()

    # https://deepgraphlearning.github.io/project/wikidata5m
    # download("https://www.dropbox.com/s/lnbhc8yuhit4wm5/wikidata5m_alias.tar.gz?dl=1")
    # download("https://www.dropbox.com/s/7jp4ib8zo3i6m10/wikidata5m_text.txt.gz?dl=1")

    # ('Q7594088', 'St Magnus the Martyr, London Bridge is a Church of England church and parish within the City of
    # London. The church, which is located in Lower Thames Street near The Monument to the Great Fire of London,
    # is part of the Diocese of London and under the pastoral care of the Bishop of Fulham. It is a Grade I listed
    # building. The rector uses the title "Cardinal Rector", being the last remaining cleric in the Church of England
    # to use the title Cardinal.St Magnus lies on the original alignment of London Bridge between the City and
    # Southwark. The ancient parish was united with that of St Margaret, New Fish Street, in 1670 and with that of St
    # Michael, Crooked Lane, in 1831. The three united parishes retained separate vestries and churchwardens. Parish
    # clerks continue to be appointed for each of the three parishes.St Magnus is the guild church of the Worshipful
    # Company of Fishmongers and the Worshipful Company of Plumbers, and the ward church of the Ward of Bridge and
    # Bridge Without. It is also twinned with the Church of the Resurrection in New York City.Its prominent location
    # and beauty have prompted many mentions in literature.  In Oliver Twist Charles Dickens notes how,
    # as Nancy heads for her secret meeting with Mr Brownlow and Rose Maylie on London Bridge, "the tower of old
    # Saint Saviour\'s Church, and the spire of Saint Magnus, so long the giant-warders of the ancient bridge,
    # were visible in the gloom". The church\'s spiritual and architectural importance is celebrated in the poem The
    # Waste Land by T. S. Eliot, who wrote, "the walls of Magnus Martyr hold/Inexplicable splendour of Ionian white
    # and gold". He added in a footnote that "the interior of St. Magnus Martyr is to my mind one of the finest among
    # Wren\'s interiors". One biographer of Eliot notes that at first he enjoyed St Magnus aesthetically for its
    # "splendour"; later he appreciated its "utility" when he came there as a sinner.')
    # iterator = iter_data('data/wikidata5m_text.txt.gz')
    # for row in iterator:
    #     print(row)
    #     break

    # ('Q5196650', 'Cut Your Hair', 'cut your hair')
    # iterator = iter_data('data/wikidata5m_alias.tar.gz')
    # for row in iterator:
    #     print(row)
    #     break

    alias_cache = DiskSearch('data/wikidata-5m-alias.cache')
    dataset = DiskSearch('data/wikidata-5m-dataset.cache')
    # alias_cache.write(((id, data) for id, *data in iter_data( 'data/wikidata5m_alias.tar.gz')))
    # iterator = (
    #     (
    #         qid,
    #         {
    #             "id": qid,
    #             "text": "\t".join(text),
    #             "aliases": alias_cache.read(qid),
    #         },
    #     )
    #     for qid, *text in iter_data('data/wikidata5m_text.txt.gz')
    # )
    # dataset.write(iterator)

    mapping = {}
    for key, data in tqdm(dataset.iter(), total=5_000_000):
        for lookup in (data['aliases'] or []):
            lookup = lookup.lower()
            ids = mapping.get(lookup)
            if ids is None:
                mapping[lookup] = {key}
            else:
                ids.add(key)

    dataset_aliases = DiskSearch('data/wikidata-5m-dataset.aliases.cache')
    dataset_aliases.write(tqdm(mapping.items()))

    print(dataset_aliases['Donald Trump'])
    print(dataset_aliases['Estonia'])
