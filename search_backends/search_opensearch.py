from opensearchpy import OpenSearch

# {
# 'aliases-a': 0.375, 
# 'aliases-b': 0.65, 
# 'aliases-c': 0.225, 
# 'title-a': 0.05, 
# 'title-b': 0.525, 
# 'title-c': 0.17500000000000002, 
# 'factor-a': 6.1000000000000005, 
# 'factor-modifier': 'log2p', 
# 'factor-mode': 'sum', 
# 'item-mode': 'max',
# 'aliases-kw': 0.57, 'title-kw': 1.45,
# 'factor-a': 1.55, 'aliases-kw': 0.89, 'title-kw': 0.40
#  'aliases-a': 0.35476046472355094, 
#  'aliases-b': 0.8595323663508687, 
#  'aliases-c': 0.18352243055816408, 
#  'title-a': 0.06916536446815502, 
#  'title-b': 0.4594429215660488, 
#  'title-c': 0.4471684365493579, 
#  'factor-a': 6.6009803577143975, 
#  'aliases-kw': 0.9323488758103362, 
#  'title-kw': 0.0010505435695555843,
# }

search_item_template = lambda names, mapping: {
        "function_score": {
            "query": {
                "bool": {
                    "should": [
                        {
                            "function_score": {
                                "query": {
                                    "bool": {
                                        "should": [
                                            {"match": {"aliases": {"query": name, "boost": mapping.get("aliases-a", 0.35)}}},
                                            {"match": {"aliases": {"query": name,"fuzziness": "AUTO","boost": mapping.get("aliases-b", 0.86),}}},
                                            {"match": {"aliases": {"query": name,"boost": mapping.get("aliases-c", 0.18),"analyzer": "english",}}},
                                            {"match": {"title": {"query": name, "boost": mapping.get("title-a", 0.07)}}},
                                            {"match": {"title": {"query": name,"boost": mapping.get("title-b", 0.46),"analyzer": "english",}}},
                                            {"match": {"title": {"query": name,"fuzziness": "AUTO","boost": mapping.get("title-c", 0.45),}}},
                                            # exact lookups
                                            {"term": {"title.keyword": {"value": name, "boost": mapping.get("title-kw", 0.00)}}},
                                            {"term": {"aliases": {"value": name.lower(), "boost": mapping.get("aliases-kw", 0.93)}}},
                                        ]
                                    }
                                },
                                "score_mode": "sum"  # Combine scores from multiple match statements
                            }
                        }
                        for name in names
                    ],
                }
            },
            "score_mode": mapping.get("item-mode", "max"),  # Focus on the maximum score among sub-queries
        }
}

search_query_template = lambda names, mapping: {
    "query": {
        "function_score": {
            "query": search_item_template(names, mapping),
            "functions": [
                {
                    "field_value_factor": {
                        "field": "popularity",  # Field containing popularity score
                        "modifier": mapping.get("factor-modifier", "log2p"),  # Apply logarithmic function to popularity score
                        "factor":  mapping.get("factor-a", 6.6),  # Adjust the boosting factor based on your requirements
                    }
                }
            ],
            "score_mode": mapping.get("factor-mode", "sum"),
        },
    },
}

#             source           method  search_limit  candidates    rank  found  score   index
# 0  wikidata-weight     TantivitySearch        8192.0    10579.83  120.36   0.92  57.23  177.82
# 0  wikidata-lessfields  TantivitySearch        8192.0      9069.4  93.38   0.93  31.41  256.49
names = ["estonia"]
# names = ["eesti"]
# names = ["Ingenuity"]
# names = ['Ingenuity', 'Ingenuity']
# names = ['nafo']
# names = ["electric lighting"]
# names = ["Madonna"]
search_query = search_query_template(
    names, 
    {}
    # {
    # 'aliases-a': 0.65, 'aliases-b': 0.9, 'aliases-c': 0.25, 'title-a': 0.875, 'title-b': 0.55, 'title-c': 0.35000000000000003, 'factor-a': 1.0},
    # {'aliases-a': 0.225, 'aliases-b': 0.7000000000000001, 'aliases-c': 0.42500000000000004, 'title-a': 0.4, 'title-b': 0.9750000000000001, 'title-c': 0.025, 'factor-a': 1.05, 'factor-modifier': 'ln2p', 'factor-mode': 'sum'}
    # {'aliases-a': 0.375, 'aliases-b': 0.65, 'aliases-c': 0.225, 'title-a': 0.05, 'title-b': 0.525, 'title-c': 0.17500000000000002, 'factor-a': 6.1000000000000005, 'factor-modifier': 'log2p', 'factor-mode': 'sum', 'item-mode': 'max'},
)

client = OpenSearch(
    hosts=["localhost"],
    http_auth=("admin", "7Tr0ngP@ssw0rdwget"),
    scheme="http",
    port=9200,
)
index_name = "search_index_v2"
response = client.search(index=index_name, body=search_query, size=32)
print(response["hits"]["total"])

hits = response["hits"]["hits"]
for i, hit in enumerate(hits):
    print(i, round(hit["_score"], 1), hit["_id"], hit["_source"])
    if hit["_id"] in ["Q1326621", "Q191", "Q1744"]:
        break

# exit()

from typing import List
import os
import shutil
from typing import List

from tqdm.auto import tqdm
import tantivy

from diskstorage import DiskSearch
from noun_phases import get_phrases, get_noun_phrases
from search_dataset.base import BaseSearch
from utils import reset_working_directory

alias_index = DiskSearch('data/wikidata-v3.aliases-lc.cache')
alais_dataset = DiskSearch(f'data/wikidata-v3.cache')

import faiss
from utils.embedding import load_use4, load_st1

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
            distance = D[i, j]
            if distance < threshold:
                r_key = index_cache[str(neighbor_index)]
                results[r_key] = {
                    'wikidata_id': r_key,
                    'distance': distance,
                    # 'score': 999_999,
                    **alais_dataset[r_key]
                }

    output = sorted(results.values(), key=lambda d: d['distance'])
    output = list({d['wikidata_id']: d for d in output}.values())
    output = [{**d, 'rank': i} for i, d in enumerate(output)]
    return output


def alias_search(
        phrases: List[str],
        search_limit: int = 32,
):
    output = []
    for p in phrases:
        p = p.lower()
        for hit in alias_index[p] or []:
            output.append({
                "wikidata_id": hit,
                'alias': True,
                "rank": 0,
                **alais_dataset[hit]
            })
    return output


class EmbeddingSearch(BaseSearch):
    searcher: object
    field_boosts: dict = {
        # 'support': -4.36, 'distance': 6.33, 'alias': 5.64, 'rank': 0
        'support': -3.7378973216194193, 'distance': -1.7104238360343302, 'alias': -6.556479233679184
    }

    def sort_key(self, row):
        score = 0
        if row['support'] is not None:
            score += row['support'] * self.field_boosts.get('support', 0)
        if row['rank'] is not None:
            score += (row['rank'] / self.search_limit) * self.field_boosts.get('rank', 0)
        if row['distance'] is not None:
            score += row['distance'] * self.field_boosts.get('distance', 0)
        if row['alias'] is not None:
            score += row['alias'] * self.field_boosts.get('alias', 0)
        return score

    def search(self, row):
        mention_name, annotated_text, mention_llm = row['name'], row['text'], row['llm']
        results = []
        phrases = get_phrases(mention_name, annotated_text)

        results += alias_search(phrases, self.search_limit)#[:self.search_limit]

        # mention_class = row['class']
        # mention_name = max(phrases, key=lambda p: (len(p), p[0].isupper()))
        # mention_llm = mention_llm.split(":")[-1]
        text_candidates = [
            mention_name + ", " + mention_llm[0].lower() + mention_llm[1:].strip(),
            # mention_llm,
            # annotated_text,
            # f"{mention_name}, {mention_class}",
            # ", ".join(get_noun_phrases(annotated_text)),
        ]
        # text_candidates = [c[0].upper() + c[1:] for c in text_candidates]
        # text_candidates = [s.replace("(", "").replace(")", "").split(': ')[-1] for s in text_candidates]

        results += emb_search(text_candidates, self.search_limit, threshold=1.00)[:self.search_limit]
        # results = list({d['wikidata_id']: d for d in results}.values())

        # print(mention_name, phrases)
        # results = [r for r in results if r['count_languages'] > 0]

        query = search_query_template(phrases, self.field_boosts)
        response = self.searcher.search(index=index_name, body=query, size=self.search_limit, _source=False)
        hits = response["hits"]["hits"]
        for i, hit in enumerate(hits):
            results.append({
                "wikidata_id": hit["_id"],
                "score": hit["_score"],
                "rank": i,
            })

        # results = sorted(results, key=lambda d: d.get('score', 999_999), reverse=True)

        found = dict(Counter(d['wikidata_id'] for d in results).most_common())
        # results = [d for d in results if found[d['wikidata_id']] > 1]

        temp = {}
        for d in results:
            entry = temp.get(d['wikidata_id'])
            if not entry:
                entry = temp[d['wikidata_id']] = d

            entry['support'] = found[entry['wikidata_id']]
            entry['rank'] = min(entry['rank'], d['rank'])
            entry['distance'] = min(entry.get('distance') or -1, d.get('distance') or -1)
            entry['distance'] = None if entry['distance'] == -1 else entry['distance']
            entry['alias'] = bool(entry.get('alias') or d.get('alias'))

        results = list(temp.values())
        # results = sorted(results, key=lambda d: (-d['support'], d['rank']))
        results = sorted(results, key=self.sort_key)
        results = results[:self.search_limit]

        # results = list({d['wikidata_id']: d for d in results}.values())
        # results = sorted(results, key=lambda d: d.get('rank', 999_999))
        return results

from collections import Counter
from search_dataset import SEARCH_DATASET, evaluate
import pandas as pd

search_limit = 96
# search_limit = 1024 * 8
dataset = SEARCH_DATASET[::]
# dataset = [d for d in dataset if d['id'] in ['Q48814715', 'Q113640505']]
data = []
data += evaluate(
    dataset,
    EmbeddingSearch(
        searcher=client,
        search_limit=search_limit,
        name='wikidata-weight',
    ),
    search_limit,
)

data = pd.DataFrame.from_records(data)
# print(data[~data.found]['name'].to_list())
print(data.groupby(['source', "method"]).mean(numeric_only=True).reset_index().round(2))

# exit()

import optuna
from functools import lru_cache

@lru_cache
def get_searcher():
    client = OpenSearch(
        hosts=["localhost"],
        http_auth=("admin", "7Tr0ngP@ssw0rdwget"),
        scheme="http",
        port=9200,
    )
    return client


def objective(trial: optuna.Trial):

    data = evaluate(
        dataset[::],
        EmbeddingSearch(
            searcher=get_searcher(),
            search_limit=search_limit,
            name='wikidata-weight',
            field_boosts = {
                "support": trial.suggest_float("support", -10.0, 1.0, step=0.1),
                "distance": trial.suggest_float("distance", -10.0, 1.0, step=0.1),
                "alias": trial.suggest_float("alias", -10.0, 1.0, step=0.1),
                # "rank": trial.suggest_float("rank", -10.0, 10.0),
            },
        ),
        search_limit,
    )
    data = pd.DataFrame.from_records(data)
    score = data['rank'].mean()
    found = data['found'].mean()
    # return round(found, 3)
    # return round(score, 3)
    return (round(found, 3), round(score, 3))

search_limit = 128
study = optuna.create_study(directions=["maximize", "minimize"])#directions=["maximize", "minimize"])
study.optimize(objective, n_trials=100)

for trial in study.best_trials:
    print(trial.values, trial.params)
