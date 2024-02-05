from os.path import isfile

from tqdm import tqdm
import requests

from utils import reset_working_directory

reset_working_directory()


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


# https://deepgraphlearning.github.io/project/wikidata5m
# download("https://www.dropbox.com/s/lnbhc8yuhit4wm5/wikidata5m_alias.tar.gz?dl=1")
# download("https://www.dropbox.com/s/7jp4ib8zo3i6m10/wikidata5m_text.txt.gz?dl=1")


import gzip
import os


def iter_data(file_name):
    total_size = os.path.getsize(file_name)

    with tqdm(total=total_size, unit="B", unit_scale=True, desc="emb", smoothing=0.1, maxinterval=0.5) as progress_bar:
        old = 0
        with gzip.open(file_name, 'rb') as file:
            for line in file:
                new_start_pos = file.fileobj.tell()
                progress_bar.update(new_start_pos - old)
                old = new_start_pos

                line = line.decode('utf-8')
                # print('got line', line)
                qid, *text = line.split("\t")
                text = " ".join(text)
                yield {
                    'id': qid,
                    'text': text,
                }


import threading
import queue
import time


class FileReaderThread(threading.Thread):
    def __init__(self, reader, chunk_size):
        super().__init__()
        self.reader = reader
        self.chunk_size = chunk_size
        self.queue = queue.Queue()

    def run(self):
        for item in self.reader:
            self.queue.put(item)
            while self.queue.qsize() >= chunk_size:
                time.sleep(0.05)
        self.queue.put(None)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.join()

    def __iter__(self):
        return self

    def __next__(self):
        item = self.queue.get()
        if item is None:
            raise StopIteration
        return item


import multiprocessing
import queue
import time


class FileReaderProcess(multiprocessing.Process):
    def __init__(self, reader, chunk_size):
        super().__init__()
        self.reader = reader
        self.chunk_size = chunk_size
        self.queue = multiprocessing.Queue()

    def run(self):
        for item in self.reader:
            self.queue.put(item)
            while self.queue.qsize() >= self.chunk_size:
                time.sleep(0.05)
        self.queue.put(None)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.join()

    def __iter__(self):
        return self

    def __next__(self):
        item = self.queue.get(block=True)
        if item is None:
            raise StopIteration
        return item


from itertools import islice


def chunk(iterator, size=32):
    while True:
        out = []
        for e in iterator:
            out.append(e)
            if len(out) >= size:
                break
        if out:
            yield out
        else:
            break

file_name = 'data/wikidata5m_text.txt.gz'
iterator = iter_data(file_name)

# c = 0
# queue = queue.Queue()
# with FileReaderProcess(iterator, 1024) as iterator:
#     time.sleep(0.5)
#     for e in iterator:
#         c += 1
# #     queue.put(e)
# #     queue.qsize()
#
# print(c)
# exit()
import os

os.environ["CUDA_VISIBLE_DEVICES"] = "1"


def load_st1():
    from sentence_transformers import SentenceTransformer, util
    model = SentenceTransformer('all-MiniLM-L6-v2')
    encoder = model.encode
    return encoder, model


def load_use4():
    import tensorflow as tf
    import tensorflow_hub as hub
    with tf.device('GPU:1'):
        encoder = embed = hub.load("https://tfhub.dev/google/universal-sentence-encoder/4")
    return encoder, embed


file_name = 'data/wikidata5m_text.txt.gz'
iterator = iter_data(file_name)

chunk_size = 4096
chunk_size = 4096 * 8
# chunk_size = 1024
# with FileReaderProcess(iterator, chunk_size * 8) as iterator:
iterator = list( iter_data(file_name))
# chunks = chunk(iterator, chunk_size)
chunks = [iterator[i:i+chunk_size] for i in tqdm(range(0, len(iterator), chunk_size))]


encoder, _model = load_use4()
# encoder, _model = load_st1()

for lines in tqdm(list(chunks)):
    text = [" ".join(line['text'].split()[:128]) for line in lines]
    embs = encoder(text)
