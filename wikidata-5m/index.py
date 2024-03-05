import os
from itertools import islice
from random import randint

from tqdm.auto import tqdm

from diskstorage import DiskSearch
from utils import reset_working_directory, chunking
from multiprocessing import Lock, Pool

from utils.embedding import load_use4

reset_working_directory()



def process(chunk):
    encoder, _model, dim = load_use4()
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
    return ids, embs.numpy()


dataset = DiskSearch('data/wikidata.cache')

embedding_cache = 'data/wikidata.emb.v3.cache'
index_cache = 'data/wikidata.index.v3.cache'
vector_cache = 'data/test.v3.ann'

dataset_index = DiskSearch(index_cache)

# model_loader = load_st1
model_loader = load_use4


import numpy as np

# total_rows = len(tuple(dataset.keys()))
# rows = (
#     row
#     for key, row in tqdm(dataset.iter(), total=total_rows)
#     if row['text'] and key.startswith('Q')  # or row['aliases']
# )
# iterator = chunking(
#     rows,
#     size=256,
# )
# embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='w+', shape=(total_rows * 2, 512))

# all_ids = {}
# with Pool(2) as p:
#     for ids, embs in p.imap(process, islice(iterator, None)):
#         for i, (qid, v) in enumerate(zip(ids, embs), start=len(all_ids)):
#             all_ids[i] = qid
#             embeddings_fp[i] = v

# dataset_index.write((str(i), q) for i, q in all_ids.items())

# import numpy as np
# total_rows = 1000
# embeddings = np.memmap("embedding_cache.dat", dtype='float32', mode='r', shape=(total_rows, 512))
# for i in range(total_rows):
#     embedding = embeddings[i]

import numpy as np
from annoy import AnnoyIndex
keys = tuple(dataset_index.keys())
total_rows = len(keys)
embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='r', shape=(total_rows, 512))
print(embeddings_fp.shape)
try:
    os.remove(vector_cache)
except OSError:
    pass

import faiss
nlist = 512  # Number of clusters
quantizer = faiss.IndexFlatIP(512)
index = faiss.IndexIVFPQ(quantizer, 512, nlist, 8, 8)  # Using IVFPQ index
# index = faiss.index_factory(512, "IVF256,Flat")
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

# t = AnnoyIndex(512, 'dot')
# t.on_disk_build(vector_cache)
# for i in tqdm(keys):
#     i = int(i)
#     print(i)
#     # t.add_item(i, embeddings_fp[i])
# t.build(8, n_jobs=-1)
# t.save(vector_cache)

# from annoy import AnnoyIndex
#
# u = AnnoyIndex(512, 'dot')
# u.load(vector_cache)
#
# for i in range(2, 100):
#     key = f"Q{i}"
#     if not dataset[key]:
#         continue
#     print()
#     print()
#     print(dataset[key]['text'])
#     emb = load_use4()[0]([dataset[key]['text']])[0]
#     print(emb.shape, key)
#
#     idx = u.get_nns_by_vector(emb, 16)
#     print(idx)
#     for i in idx:
#         r_key = dataset_index[str(i)]
#         print(dataset[r_key]['text'])
