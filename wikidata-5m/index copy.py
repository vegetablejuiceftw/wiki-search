import os
from itertools import islice
from random import randint

from tqdm.auto import tqdm

from diskstorage import DiskSearch
from utils import reset_working_directory
from multiprocessing import Lock, Pool
import numpy as np
from utils.embedding import load_st1

reset_working_directory()


def chunking(iterator, size=32):
    iterator = iter(iterator)
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


def process(chunk):
    encoder, _model, dim = model_loader()
    text, ids = [], []

    for c in chunk:
        candidates = {
            f"{max(c['aliases'], key=len)}, {c['text']}"
            if c['text']
            else max(c['aliases'], key=len),
            c['text']
            if c['text']
            else max(c['aliases'], key=len),
        }
        for t in candidates:
            t = " ".join(t.split()[:64])
            text.append(t)
            ids.append(c['id'])
    embs = encoder(text)
    if not isinstance(embs, np.ndarray):
        embs = embs.numpy()
    return ids, embs


dataset = DiskSearch('data/wikidata.cache')

embedding_cache = 'data/wikidata.emb.v4.cache'
index_cache = 'data/wikidata.index.v4.cache'
vector_cache = 'data/test.v4.ann'

dataset_index = DiskSearch(index_cache)

model_loader, model_dim = load_st1, 384


import numpy as np

total_rows = len(tuple(dataset.keys()))
rows = (
    row
    for key, row in tqdm(dataset.iter(), total=total_rows, smoothing=0.1, )
    if row['text'] and key.startswith('Q')  # or row['aliases']
)
iterator = chunking(
    rows,
    size=256 * 2,
)
embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='w+', shape=(total_rows * 2, model_dim))

all_ids = {}
with Pool(3) as p:
    for ids, embs in p.imap(process, islice(iterator, None)):
        for i, (qid, v) in enumerate(zip(ids, embs), start=len(all_ids)):
            all_ids[i] = qid
            embeddings_fp[i] = v

dataset_index.write((str(i), q) for i, q in all_ids.items())


import numpy as np
from annoy import AnnoyIndex
keys = tuple(dataset_index.keys())
total_rows = len(keys)
embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='r', shape=(total_rows, model_dim))
print(embeddings_fp.shape)
try:
    os.remove(vector_cache)
except OSError:
    pass

import faiss
nlist = 256  # Number of clusters
quantizer = faiss.IndexFlatIP(model_dim)
index = faiss.IndexIVFPQ(quantizer, model_dim, nlist, 8, 8)  # Using IVFPQ index
# index = faiss.index_factory(model_dim, "IVF256,Flat")
# index.train(embeddings_fp)

# Batch size for adding vectors to the index
batch_size = 1024 * 1024

# Train the index in batches
for i in tqdm(range(0, total_rows, batch_size)):
    batch = embeddings_fp[i:i+batch_size]
    index.train(batch)
import gc
gc.collect()

batch_size = 1024 * 16

# Add vectors to the index in batches
for i in tqdm(range(0, total_rows, batch_size)):
    batch = embeddings_fp[i:i+batch_size]
    index.add(batch)

# Save the index to disk
faiss.write_index(index, vector_cache)
