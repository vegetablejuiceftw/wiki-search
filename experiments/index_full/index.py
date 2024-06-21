import os
import types
from itertools import islice

from tqdm.auto import tqdm

from diskstorage import DiskSearch
from utils import reset_working_directory
import numpy as np
from utils.embedding import load_st1, load_fe_st1, load_use4
import json
from torch.multiprocessing import Pool, set_start_method

set_start_method("spawn", force=True)

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

    if isinstance(embs, types.GeneratorType):
        embs = list(embs)

    if hasattr(embs, "numpy"):
        embs = embs.numpy()

    return ids, embs


model_loader, model_dim = load_st1, 384
# model_loader, model_dim = load_use4, 512


if __name__ == '__main__':

    embedding_cache = 'data/embeddings/v7?/wikidata.emb.cache'
    index_cache = 'data/embeddings/v7?/wikidata.index.cache'
    inverse_cache = 'data/embeddings/v7?/wikidata.inverse.cache'

    os.makedirs(os.path.dirname(embedding_cache), exist_ok=True)

    dataset = DiskSearch(f'data/wikidata-v3.cache')

    rows = (
        dataset[key]
        for key in tqdm(dataset.keys(), total=27420075)
        if dataset[key]
    )
    total_rows = 27420075 # len(rows)
    print(f"Total rows: {total_rows}")

    iterator = chunking(
        rows,
        size=256 * 2,
    )
    # iterator = islice(iterator, 10)

    embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='w+', shape=(total_rows * 2, model_dim))

    all_ids = {}
    with Pool(2) as p:
        for ids, embs in p.imap_unordered(process, iterator):
            for i, (qid, v) in enumerate(zip(ids, embs), start=len(all_ids)):
                all_ids[i] = qid
                embeddings_fp[i] = v



    for ids, embs in map(process, iterator):
        for i, (qid, v) in enumerate(zip(ids, embs), start=len(all_ids)):
            all_ids[i] = qid
            embeddings_fp[i] = v


    dataset_index = DiskSearch(index_cache)
    dataset_index.write((str(i), q) for i, q in all_ids.items())

    key_index = {}
    for k, q in tqdm(dataset_index.iter(), desc='inverse map'):
        if not key_index:print([k, q])
        arr = key_index.get(q)
        if arr is None:
            key_index[q] = [k]
        else:
            arr.append(k)

    inverse_index = DiskSearch(inverse_cache)
    inverse_index.write(key_index.items())
