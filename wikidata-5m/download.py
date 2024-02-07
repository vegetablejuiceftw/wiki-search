from os.path import isfile

import gzip
import os
import tarfile

from tqdm import tqdm
import requests

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


def iter_data(file_name):
    total_size = os.path.getsize(file_name)

    with tqdm(total=total_size, unit="B", unit_scale=True, desc="emb", smoothing=0.1, maxinterval=0.5) as progress_bar:
        old = 0
        if file_name.endswith('.tar.gz'):
            with tarfile.open(file_name, 'r') as file:
                stream = file.extractfile("wikidata5m_entity.txt")
                for line in stream:
                    new_start_pos = file.fileobj.tell()
                    progress_bar.update(new_start_pos - old)
                    old = new_start_pos
                    yield tuple(r.strip() for r in line.decode('utf-8').split("\t"))
        else:
            with gzip.open(file_name, 'rb') as file:
                stream = file
                for line in stream:
                    new_start_pos = file.fileobj.tell()
                    progress_bar.update(new_start_pos - old)
                    old = new_start_pos
                    yield tuple(r.strip() for r in line.decode('utf-8').split("\t"))


if __name__ == '__main__':
    reset_working_directory()

    # https://deepgraphlearning.github.io/project/wikidata5m
    download("https://www.dropbox.com/s/lnbhc8yuhit4wm5/wikidata5m_alias.tar.gz?dl=1")
    download("https://www.dropbox.com/s/7jp4ib8zo3i6m10/wikidata5m_text.txt.gz?dl=1")

    file_name = 'data/wikidata5m_text.txt.gz'
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
    iterator = iter_data(file_name)
    for row in iterator:
        print(row)
        break

    file_name = 'data/wikidata5m_alias.tar.gz'
    # ('Q5196650', 'Cut Your Hair', 'cut your hair')
    iterator = iter_data(file_name)
    for row in iterator:
        print(row)
        break
