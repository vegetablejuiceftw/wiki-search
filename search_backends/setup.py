import os
import shutil
from typing import List

from tqdm.auto import tqdm
import tantivy

from diskstorage import DiskSearch
from noun_phases import get_phrases
from search_dataset.base import BaseSearch
from utils import reset_working_directory

reset_working_directory()

dataset = DiskSearch(f'data/wikidata-v3.cache')
print(dataset['Q191'])

rows = [
    {
        "id": "Q191",
        "title": "Estonia",
        "text": "Estonia is known for its cultural heritage",
        "aliases": [
            "Eesti"
        ],
        "popularity": 1
    },
]


# Create schema
schema_builder = tantivy.SchemaBuilder()
schema_builder.add_text_field("id", stored=True)
schema_builder.add_text_field("title")
schema_builder.add_text_field("label")
schema_builder.add_text_field("alias")
schema_builder.add_text_field("title_stem", tokenizer_name="en_stem")
schema = schema_builder.build()

index_dir = "data/tantivy-v3"
#
# if os.path.exists(index_dir):
#     print("DEL")
#     shutil.rmtree(index_dir)
#
# if not os.path.exists(index_dir):
#     print("CREATE")
#     os.mkdir(index_dir)
#
# index = tantivy.Index(schema, path=index_dir)
#
# total_rows = 27_420_075 or len(tuple(dataset.keys()))
# rows = (
#     row
#     for key, row in tqdm(dataset.iter(), total=total_rows)
#     if row['text'] and key.startswith('Q')
# )
# # Add documents to the index
# index_writer = index.writer()
# for doc in rows:
#     title = (doc['labels'] + doc['aliases'] + [doc['text']])[0]
#     index_writer.add_document(
#         tantivy.Document(
#             id=doc["id"],
#             alias=doc["aliases"],
#             label=doc["labels"],
#             title=title,
#             title_stem=title,
#         )
#     )
# index_writer.commit()
# index.reload()

index = tantivy.Index(schema, path=index_dir)

searcher = index.searcher()

# Search for documents
for query in [
    index.parse_query("Estonia", ["alias"]),
    index.parse_query("Estonia", ["label"]),
    index.parse_query("Estonia", ["title"]),
    index.parse_query("Estonia", ["title", "label"]),
    index.parse_query("Estonia", ["title", "label", "alias"]),
    index.parse_query("Estonia", fuzzy_fields={"alias": (True, 1, False)}),
]:
    print("---")
    print("---")
    print(query)
    # help(searcher.search)
    for i, (score, hit) in enumerate(searcher.search(query, limit=1024 * 16).hits):
        doc = searcher.doc(hit)
        assert len(doc['id']) == 1
        qid = doc['id'][0]
        if qid == 'Q191':
            print(i, round(score, 2), qid, dataset[qid]['text'], dataset[qid]['aliases'])
            print("####")
            break

query = index.parse_query('"electric lighting"', ["title_stem"])
for i, (score, hit) in enumerate(searcher.search(query, limit=4).hits):
    doc = searcher.doc(hit)
    assert len(doc['id']) == 1
    qid = doc['id'][0]
    print(i, round(score, 2), qid, dataset[qid]['text'], dataset[qid]['aliases'])

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
    field_boosts: dict = {
        "title": 1.0,
        "label": 1.0,
        "alias": 0.2,
        "title_stem": -0.0,
    }

    def search(self, row):
        mention_name, annotated_text, mention_llm = row['name'], row['text'], row['llm']

        phrases = get_phrases(mention_name, annotated_text)  # + mention_llm.split(",")
        phrases = {p: 1 for p in phrases}.keys()
        phrases = [p.strip() for p in phrases]

        results = []
        results += alias_search(phrases, self.search_limit)
        for p in phrases:
            p = " ".join(phrases)
            query = index.parse_query(
                # f'"{p}"',
                p,
                [
                    "title",
                    "label",
                    "alias",
                    "title_stem"
                ],
                # fuzzy_fields = {"alias": (True, 1, False)},
                field_boosts=self.field_boosts,
            )
            for i, (score, hit) in enumerate(searcher.search(query, limit=self.search_limit).hits):
                doc = searcher.doc(hit)
                assert len(doc['id']) == 1
                qid = doc['id'][0]
                results.append({
                    "wikidata_id": qid,
                    "score": score,
                })
            break
        results += sorted(results, key=lambda d: d.get('score', 999_999), reverse=True)
        results = list({d['wikidata_id']: d for d in results}.values())
        return results


from search_dataset import SEARCH_DATASET, evaluate
import pandas as pd

search_limit = 64
dataset = SEARCH_DATASET[:]
data = []
data += evaluate(
    dataset,
    TantivitySearch(
        searcher=searcher,
        search_limit=search_limit,
        name='wikidata-weight',
        # field_boosts={'title': -0.75, 'label': 0.65, 'alias': 0.74, 'title_stem': 0.8},
        # field_boosts={'title': 0.06000000000000005, 'label': -0.16999999999999993, 'alias': 0.3700000000000001, 'title_stem': 0.22999999999999998},
        # field_boosts={'title': 0.14000000000000012, 'label': -0.44999999999999996, 'alias': 0.97, 'title_stem': 0.24},
        # field_boosts={'title': -0.29999999999999993, 'label': -0.030000000000000027, 'alias': 0.95, 'title_stem': 0.8800000000000001},
        # field_boosts={'title': 0.31000000000000005, 'label': -0.62, 'alias': 1.0, 'title_stem': 0.6100000000000001},
        # field_boosts={'title': 0.55, 'label': -0.8, 'alias': 0.15000000000000013, 'title_stem': 0.6500000000000001},
        field_boosts={'title': -0.5, 'label': 0.15000000000000013, 'alias': 0.9500000000000002, 'title_stem': 0.40000000000000013},
    ),
    search_limit,
)

data = pd.DataFrame.from_records(data)
print(data[~data.found]['name'].to_list())
print(data.groupby(['source', "method"]).mean(numeric_only=True).reset_index().round(2))

#             source           method  search_limit  candidates  rank  found  score
# 0  wikidata-FUzzy   TantivitySearch          64.0       73.41   6.6   0.92  98.88
# 0  wikidata-simple  TantivitySearch          64.0        71.7  7.33   0.92  127.09
# 0  wikidata-weight  TantivitySearch          64.0       71.61  7.57   0.92  311.65

"""
            source           method  search_limit  candidates   rank  found  score
0  wikidata-simple  TantivitySearch          64.0       88.11  14.22   0.77  62.55
            source           method  search_limit  candidates   rank  found   score
0  wikidata-weight  TantivitySearch          64.0       88.26  14.42   0.77  124.05


            source           method  search_limit  candidates      rank  found   score
X    wikidata-title   TantivitySearch          64.0       59.34   10.92   0.68  162.37
0    wikidata-weight  TantivitySearch        8192.0     5532.35  143.98   0.86   28.00
0.1  wikidata-weight  TantivitySearch        8192.0     5532.35   97.37   0.86   31.28
0.2  wikidata-weight  TantivitySearch        8192.0     5532.35  135.18   0.89   33.62
"""

import optuna
from functools import lru_cache

@lru_cache
def get_searcher():
    index = tantivy.Index(schema, path=index_dir)
    searcher = index.searcher()
    return searcher


# {'title': -0.7584157929076399, 'label': 0.6508970641562654, 'alias': 0.7401661321905902, 'title_stem': 0.8006390306900115}

def objective(trial: optuna.Trial):

    data = evaluate(
        dataset,
        TantivitySearch(
            searcher=get_searcher(),
            search_limit=search_limit,
            name='wikidata-weight',
            field_boosts = {
                "title": trial.suggest_float("title", -1.0, 1.0, step=0.05),
                "label": trial.suggest_float("label", -1.0, 1.0, step=0.05),
                "alias": trial.suggest_float("alias", -1.0, 1.0, step=0.05),
                "title_stem": trial.suggest_float("title_stem", -1.0, 1.0, step=0.05),
            }
        ),
        search_limit,
    )
    data = pd.DataFrame.from_records(data)
    score = data['rank'].mean()
    found = 1 - data['found'].mean()
    # return round(found * 100, 0) * 10000 + round(score, 0)
    return round(found, 3)

search_limit = 64
study = optuna.create_study()#directions=["maximize", "minimize"])
# study.enqueue_trial({"title": 1.0,"label": 1.0,"alias": 0.2,"title_stem": -0.0,})
# study.enqueue_trial({'title': -0.75, 'label': 0.65, 'alias': 0.74, 'title_stem': 0.80})
study.optimize(objective, n_trials=100, n_jobs=2)

for trial in study.best_trials:
    print(trial.value, trial.params)

# 0.143 {'title': 0.22, 'label': 0.12, 'alias': 0.66, 'title_stem': 0.83}
# 0.045 {'title': 0.31000000000000005, 'label': -0.62, 'alias': 1.0, 'title_stem': 0.6100000000000001}
# 0.068 {'title': 0.55, 'label': -0.8, 'alias': 0.15000000000000013, 'title_stem': 0.6500000000000001}


"""
docker start  opensearch \
    -p 9200:9200 -p 9600:9600 \
    -e "discovery.type=single-node" \
    opensearchproject/opensearch
docker run -d --name opensearch \
    -p 9200:9200 -p 9600:9600 \
    -e "discovery.type=single-node" \
    opensearchproject/opensearch

"""