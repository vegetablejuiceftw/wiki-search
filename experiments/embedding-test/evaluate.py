import json

import faiss
import pandas as pd
from diskstorage import DiskSearch
from noun_phases import get_phrases
from search_dataset import SEARCH_DATASET, evaluate
from search_dataset.base import BaseSearch
from tqdm.auto import tqdm
from typing import List

import os
import shutil

from utils import reset_working_directory
from utils.embedding import load_st1, load_use4
from typing import List, Dict, Union

alais_dataset = DiskSearch(f'data/wikidata-v3.cache')
emb_function = load_st1

index = faiss.read_index('data/emb_index.v4.faiss')
index_cache = DiskSearch('data/wikidata.index.v4.cache')

def emb_search(text: list, search_limit: int, threshold = 0.95):
    emb = emb_function()[0](text)
    D, I = index.search(emb, search_limit)

    results = {}
    for i in range(len(text)):
        for j in range(search_limit):
            neighbor_index = I[i, j]
            distance = D[i, j].item()
            if distance < threshold:
                r_key = index_cache[str(neighbor_index)]
                results[r_key] = {
                    'wikidata_id': r_key,
                    'distance': distance,
                    'score': 0,
                    **alais_dataset[r_key]
                }

    output = sorted(results.values(), key=lambda d: d['distance'])
    output = list({d['wikidata_id']: d for d in output}.values())
    return output


search_limit = 32
# search_limit = 1024 * 8
dataset = SEARCH_DATASET[:32]


import numpy as np
from sentence_transformers import SentenceTransformer, util

# index_arr = list(tqdm(index_cache.iter()))
# temp_key_index = {}
# for i, k in index_arr:
#     try:
#         temp_key_index[k].append(i)
#     except:
#         temp_key_index[k] = [i]
# total_rows = len(index_arr)

key_index = DiskSearch("data/key_index.cache")
# key_index.write((q, arr) for q, arr in temp_key_index.items())

# key_index = {index_cache[str(i)]: i for i in [19558026, 10025441, 21449255, 19008809, 13493230, 18890981, 13329544, 40475378, 29674315, 20053998]}
total_rows = 44888496

encoder, model, model_dim = emb_function()
print(total_rows, model_dim)

embedding_cache = 'data/embeddings/wikidata.emb.v4.npy'
embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='r', shape=(total_rows, model_dim))

data = []
for row in tqdm(dataset):
    mention_llm = row['llm'].split(":")[-1].strip()
    # mention_llm = row['name'] + ", " + mention_llm[0].lower() + mention_llm[1:].strip()
    mention_llm = mention_llm[0].upper() + mention_llm[1:]
    print(mention_llm)
    mention_emb = encoder([mention_llm])[0]

    key = row['id'].split(';')[0]

    for i in key_index[key] or []:
        i = int(i)
        file_emb = embeddings_fp[i]
        score = util.dot_score(mention_emb, file_emb)[0].item()
        data.append({
            'score': score,
            'score2': None,
            'correct': True,
        })

    # text = alais_dataset[key]['text']
    c = alais_dataset[key]
    text = f"{max(c['aliases'], key=len)}, {c['text']}"

    constructed_emb = encoder([text])[0]
    score2 = util.dot_score(mention_emb, constructed_emb)[0].item()

    data.append({
        'key': key,
        'name': row['name'],
        'mention_llm': mention_llm,
        'text': text,
        'score': None,
        'score2': score2,
        'correct': True,
    })

for key, mention_llm in [
    ['Q22284644', 'The process of applying nutrients to soil or plants to promote growth and development.'],
    ['Q1798173', 'The player or piece in a game that has the highest rank and can make the most moves.'],
    ['Q1204348', 'The ruler of a kingdom or empire, typically by hereditary right.'],
    ['Q26273768', 'A winged mythical creature from Greek mythology, associated with heroes such as Hercules and Perseus.'],
    ['Q123784966', 'A company that develops advanced artificial intelligence technologies and products.'],
    ['Q1755454', 'A philosophical principle concerning the fundamental conditions for the existence of intelligent life.'],
    ['Q5494814', 'An AI language model developed by Anthropic that demonstrates significant advancements in machine learning.'],
    ['Q5045802', 'Accumulation of small fibrous particles that builds up in clothes dryers and ducts, potentially posing a fire hazard.'],
]:
    mention_emb = encoder([mention_llm])[0]

    for i in key_index[key]:
        i = int(i)
        file_emb = embeddings_fp[i]
        score = util.dot_score(mention_emb, file_emb)[0].item()
        data.append({
            'score': score,
            'score2': None,
            'correct': False,
        })

    c = alais_dataset[key]
    text = f"{max(c['aliases'], key=len)}, {c['text']}"

    constructed_emb = encoder([text])[0]
    score2 = util.dot_score(mention_emb, constructed_emb)[0].item()

    data.append({
        'score': None,
        'score2': score2,
        'correct': False,
    })

data = pd.DataFrame.from_records(data)
print(data.sort_values('score'))
print(data.groupby(['correct']).mean(numeric_only=True).reset_index().round(2))
# print(data.score.mean().round(2), data.score.min().round(2), data.score.max().round(2))




class EmbeddingSearch(BaseSearch):

    def search(self, row):
        mention_name, annotated_text, mention_llm = row['name'], row['text'], row['llm']
        mention_class = row['class']

        mention_llm = mention_llm.split(":")[-1]
        text_candidates = [
            mention_name + ", " + mention_llm[0].lower() + mention_llm[1:].strip(),
            # mention_llm.split(":")[-1].strip(),
            # mention_llm,
            # *mention_llm.split(", "),
            # annotated_text,
            # f"{mention_name}, {mention_class}",
            # ", ".join(get_noun_phrases(annotated_text)),
        ]
        text_candidates = [c[0].upper() + c[1:] for c in text_candidates]

        text_candidates = [s.replace("(", "").replace(")", "").split(': ')[-1] for s in text_candidates]
        print("text_candidates", text_candidates)

        results = emb_search(text_candidates, self.search_limit, threshold=1.00)#[:self.search_limit]

        for r in results[:1]:
            r = {
                "id": r['id'],
                "label": r['labels'][0],
                "distance": r['distance'],
                "correct": r['wikidata_id'] in row['id'],
            }
            print(r)
            # print([r['id'], mention_llm])

        results = list({d['wikidata_id']: d for d in results}.values())
        return results


# dataset = [d for d in dataset if d['id'] in ['Q48814715', 'Q113640505']]
data = []
data += evaluate(
    dataset[:],
    EmbeddingSearch(
        search_limit=search_limit,
        name='wikidata-weight',
    ),
    search_limit,
)

data = pd.DataFrame.from_records(data)
print(data[~data.found]['name'].to_list())
print(data.groupby(['source', "method"]).mean(numeric_only=True).reset_index().round(2))

exit()

import optuna
from functools import lru_cache


def objective(trial: optuna.Trial):

    data = evaluate(
        dataset,
        EmbeddingSearch(
            searcher=get_searcher(),
            search_limit=search_limit,
            name='wikidata-weight',
        ),
        search_limit,
    )
    data = pd.DataFrame.from_records(data)
    score = data['rank'].mean()
    found = data['found'].mean()
    return (round(found, 3), round(score, 3))

search_limit = 32
study = optuna.create_study(directions=["maximize", "minimize"])
study.optimize(objective, n_trials=100)

for trial in study.best_trials:
    print(trial.values, trial.params)
