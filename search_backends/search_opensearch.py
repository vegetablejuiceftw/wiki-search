from opensearchpy import OpenSearch



search_query_template = lambda name, mapping: {
    "query": {
        "function_score": {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"aliases": {"query": name, "boost": mapping.get("aliases-a", 1.0)}}},
                        {
                            "match": {
                                "aliases": {
                                    "query": name,
                                    "fuzziness": "AUTO",
                                    "boost": mapping.get("aliases-b", 1.0),
                                }
                            }
                        },
                        {
                            "match": {
                                "aliases": {
                                    "query": name,
                                    "boost": mapping.get("aliases-c", 1.0),
                                    "analyzer": "english",
                                }
                            }
                        },
                        # {
                        #     "match": {
                        #         "aliases": {
                        #             "query": name,
                        #             "fuzziness": "AUTO",
                        #             "boost": mapping.get("aliases-a", 1.0),
                        #             "analyzer": "english",
                        #         }
                        #     }
                        # },
                        {"match": {"title": {"query": name, "boost": mapping.get("title-a", 1.0)}}},
                        {
                            "match": {
                                "title": {
                                    "query": name,
                                    "boost": mapping.get("title-b", 1.0),
                                    "analyzer": "english",
                                }
                            }
                        },
                        {
                            "match": {
                                "title": {
                                    "query": name,
                                    "fuzziness": "AUTO",
                                    "boost": mapping.get("title-c", 1.0),
                                }
                            }
                        },
                        # {"match": {"text": {"query": name, "boost": mapping.get("aliases-a", 1.0)}}},
                        # {
                        #     "match": {
                        #         "text": {
                        #             "query": name,
                        #             "boost": mapping.get("aliases-a", 1.0),
                        #             "analyzer": "english",
                        #         }
                        #     }
                        # },
                        # {
                        #     "match": {
                        #         "text": {
                        #             "query": name,
                        #             "fuzziness": "AUTO",
                        #             "boost": mapping.get("aliases-a", 1.0),
                        #         }
                        #     }
                        # },
                    ]
                },
            },
            "functions": [
                {
                    "field_value_factor": {
                        "field": "popularity",  # Field containing popularity score
                        "modifier": mapping.get("factor-modifier", "log1p"),  # Apply logarithmic function to popularity score
                        "factor":  mapping.get("factor-a", 1.0),  # Adjust the boosting factor based on your requirements
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
name = "Estonia"
name = "electric lighting"
search_query = search_query_template(
    name, 
    # {'aliases-a': 0.65, 'aliases-b': 0.9, 'aliases-c': 0.25, 'title-a': 0.875, 'title-b': 0.55, 'title-c': 0.35000000000000003, 'factor-a': 1.0},
    {'aliases-a': 0.225, 'aliases-b': 0.7000000000000001, 'aliases-c': 0.42500000000000004, 'title-a': 0.4, 'title-b': 0.9750000000000001, 'title-c': 0.025, 'factor-a': 1.05, 'factor-modifier': 'ln2p', 'factor-mode': 'sum'}
)

client = OpenSearch(
    hosts=["localhost"],
    http_auth=("admin", "7Tr0ngP@ssw0rdwget"),
    scheme="http",
    port=9200,
)
index_name = "estonia_index"
index_name = "search_index"
response = client.search(index=index_name, body=search_query, size=1024)
print(response["hits"]["total"])

hits = response["hits"]["hits"]
for i, hit in enumerate(hits):
    print(i, round(hit["_score"], 1), hit["_id"], hit["_source"])
    if hit["_id"] in ["Q1326621", "Q191"]:
        break


from typing import List
import os
import shutil
from typing import List

from tqdm.auto import tqdm
import tantivy

from diskstorage import DiskSearch
from noun_phases import get_phrases
from search_dataset.base import BaseSearch
from utils import reset_working_directory

alias_index = DiskSearch('data/wikidata-v3.aliases-lc.cache')

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
            })
    return output


class TantivitySearch(BaseSearch):
    searcher: object
    field_boosts: dict = {'aliases-a': 0.225, 'aliases-b': 0.7000000000000001, 'aliases-c': 0.42500000000000004, 'title-a': 0.4, 'title-b': 0.9750000000000001, 'title-c': 0.025, 'factor-a': 1.05, 'factor-modifier': 'ln2p', 'factor-mode': 'sum'}

    def search(self, row):
        mention_name, annotated_text, mention_llm = row['name'], row['text'], row['llm']

        phrases = get_phrases(mention_name, annotated_text)  # + mention_llm.split(",")
        phrases = {p: 1 for p in phrases}.keys()
        phrases = [p.strip() for p in phrases]

        results = []
        # results += alias_search(phrases, self.search_limit)
        for p in phrases:
            # p = " ".join(phrases)
            query = search_query_template(p, self.field_boosts)
            response = self.searcher.search(index=index_name, body=query, size=self.search_limit, _source=False)
            hits = response["hits"]["hits"]
            for i, hit in enumerate(hits):
                results.append({
                    "wikidata_id": hit["_id"],
                    "score": hit["_score"],
                    # "index": i,
                })

        results += sorted(results, key=lambda d: d.get('score', 999_999), reverse=True)
        results = list({d['wikidata_id']: d for d in results}.values())
        return results


from search_dataset import SEARCH_DATASET, evaluate
import pandas as pd

search_limit = 32
# search_limit = 1024 * 8
dataset = SEARCH_DATASET[:]
data = []
data += evaluate(
    dataset,
    TantivitySearch(
        searcher=client,
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
        dataset,
        TantivitySearch(
            searcher=get_searcher(),
            search_limit=search_limit,
            name='wikidata-weight',
            field_boosts = {
                "aliases-a": trial.suggest_float("aliases-a", 0.0, 1.0, step=0.025),
                "aliases-b": trial.suggest_float("aliases-b", 0.0, 1.0, step=0.025),
                "aliases-c": trial.suggest_float("aliases-c", 0.0, 1.0, step=0.025),
                "title-a": trial.suggest_float("title-a", 0.0, 1.0, step=0.025),
                "title-b": trial.suggest_float("title-b", 0.0, 1.0, step=0.025),
                "title-c": trial.suggest_float("title-c", 0.0, 1.0, step=0.025),
                "factor-a": trial.suggest_float("factor-a", 0.0, 10.0, step=0.025),
                'factor-modifier': trial.suggest_categorical('factor-modifier', ["log2p", "sqrt", "none", "ln2p"]),
                'factor-mode': trial.suggest_categorical('factor-mode', ["sum", "avg", "multiply"]),
            },
        ),
        search_limit,
    )
    data = pd.DataFrame.from_records(data)
    score = data['rank'].mean()
    found = 1 - data['found'].mean()
    return round(found, 3)
    # return round(score, 3)

search_limit = 32
study = optuna.create_study()#directions=["maximize", "minimize"])
study.optimize(objective, n_trials=100, n_jobs=3)

for trial in study.best_trials:
    print(trial.value, trial.params)
