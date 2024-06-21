import os
from itertools import islice
from random import randint

from tqdm.auto import tqdm

from diskstorage import DiskSearch
from utils import reset_working_directory
from multiprocessing import Lock, Pool
import numpy as np
from utils.embedding import load_st1
import json

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


def get_candidates(c):
    alias = max(c['aliases'], key=len, default=None)
    text = c['text']
    candidates = {
        f"{alias}, {text}" if text else alias, 
        text if text else alias,
    } - {None}
    for t in candidates:
        t = " ".join(t.split()[:64])
        yield t    



def process(chunk):
    encoder, _model, dim = model_loader()
    text, ids = [], []

    for c in chunk:
        for t in get_candidates(c):
            text.append(t)
            ids.append(c['id'])
    
    embs = encoder(text)
    if not isinstance(embs, np.ndarray):
        embs = embs.numpy()
    return ids, embs


embedding_cache = 'data/embeddings/v5/wikidata.emb.cache'
index_cache = 'data/embeddings/v5/wikidata.index.cache'

os.makedirs(os.path.dirname(embedding_cache), exist_ok=True)

dataset_index = DiskSearch(index_cache)
model_loader, model_dim = load_st1, 384


dataset = DiskSearch(f'data/wikidata-v3.cache')
with open("top_4k.json", "r") as f:
    top_data = list(json.load(f))
    total_rows = len(top_data)

rows = (
    dataset[key]
    for key in tqdm(top_data)
    if dataset[key]
)

iterator = chunking(
    rows,
    size=256 * 2,
)

embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='w+', shape=(total_rows * 2, model_dim))

all_ids = {}
# with Pool(3) as p:
#     for ids, embs in p.imap(process, iterator):
#         for i, (qid, v) in enumerate(zip(ids, embs), start=len(all_ids)):
#             all_ids[i] = qid
#             embeddings_fp[i] = v


for ids, embs in map(process, iterator):
    for i, (qid, v) in enumerate(zip(ids, embs), start=len(all_ids)):
        all_ids[i] = qid
        embeddings_fp[i] = v

dataset_index.write((str(i), q) for i, q in all_ids.items())
