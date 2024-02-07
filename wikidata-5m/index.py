
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
