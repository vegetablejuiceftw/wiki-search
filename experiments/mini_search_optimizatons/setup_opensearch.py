import re

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import os
import shutil
from typing import List

from tqdm.auto import tqdm

from diskstorage import DiskSearch
from utils import reset_working_directory, chunking
import json

reset_working_directory()

client = OpenSearch(
    hosts=["localhost"],
    http_auth=("admin", "7Tr0ngP@ssw0rdwget"),  # Change to your admin credentials
    scheme="http",
    port=9200,
)
print(client.indices.get_alias("*"))
index_name = "search_index_v7_emb_lite"

for name in [
    index_name,
    # "search_index_v2",
]:
    try:
        client.indices.delete(index=name)
        print("DELETED", index_name)
    except Exception as e:
        print("fail", e)

# index_settings = {
#     "properties": {
#         "aliases": {
#             "type": "text",
#             "fields": {
#                 "keyword": {
#                     "type": "keyword"
#                 },
#                 "lowercase": {  # Add a sub-field for case-insensitive searching
#                     "type": "text",
#                     "analyzer": "lowercase_analyzer"
#                 }
#             }
#         }
#     },
#     "settings": {
#         "analysis": {
#             "analyzer": {
#                 "lowercase_analyzer": {
#                     "tokenizer": "keyword",  # Use keyword tokenizer to keep original token
#                     "filter": "lowercase"    # Apply lowercase filter to tokens
#                 }
#             }
#         }
#     }
# }

mapping_body = {
    "properties": {
        "wikidata_id": {"type": "keyword"},
        "popularity": {"type": "float"},
        "aliases": {
            "type": "keyword",
            "fields": {
                "text": {"type": "text"}
            }
        },
        "aliases_text": {
            "type": "text",
        },
        "vector": {
            "type": "knn_vector",
            "index": True,
            "dimension": 384,  # Specify the number of dimensions for your vector
            "method": {
                "name": "hnsw",
                "space_type": "innerproduct",
                "engine": "faiss",
                "parameters": {
                    "ef_construction": 200,
                    "m": 32,
                    "ef_search": 34,
                }
            }
        },
    }
}
settings_body = {
    "settings": {
        "index": {
            "knn": True,
            "number_of_shards": 16,
            "number_of_replicas": 0,
        }
    }
}

client.indices.create(index=index_name, body=settings_body)
client.indices.put_mapping(index=index_name, body=mapping_body)

dataset = DiskSearch(f'data/wikidata-v3.cache')
print(dataset['Q191'])
# total_rows = 27_420_075 or len(tuple(dataset.keys()))

with open("top_4k.json", "r") as f:
    rows = (
        dataset[key]
        for key in tqdm(list(json.load(f)))
        if dataset[key] # and key in ["Q22909116", "Q5287528", "Q5287524", "Q3343092", "Q1367365",]
    )

# rows = (
#     row
#     for key, row in tqdm(dataset.iter(), total=total_rows, smoothing=0.01)
#     if (
#         row['text']
#         and key.startswith('Q')
#         # and row.get("count_languages", 0) > 1
#         # and any("est" in a.lower() for a in row['labels'])
#         # and row.get("count_languages", 0) > 0 # and any(k in a.lower() for a in row['aliases'] for k in ['est',]) and row.get("count_languages", 0) > 5
#     )
# )

import numpy as np

embedding_cache = 'data/embeddings/v5/wikidata.emb.cache'
index_cache = 'data/embeddings/v5/wikidata.index.cache'

emb_index = dict(DiskSearch(index_cache).iter())
total_keys = len(emb_index)
key_index = {}
for k, q in emb_index.items():
    if not key_index:print([k, q])
    arr = key_index.get(q)
    if arr is None:
        key_index[q] = [k]
    else:
        arr.append(k)

embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='r', shape=(total_keys, 384))

import math
# # Define a generator function to yield documents in bulk
def document_generator(rows):
    global count
    for row in rows:
        text = re.sub(r"\s+", " ", row["text"])
        title = (row['labels'] + row['aliases'] + [text])[0]
        aliases = [a.lower() for a in row.get("aliases", [])]
        for key in key_index[row['id']]: 
            vector = embeddings_fp[int(key)].tolist()
            yield {
                "_index": index_name,
                "_id": f'{row["id"]}-{key}',  # Assuming "id" is unique and can be used as the document ID
                "_source": {
                    "title": title,
                    "text": text,
                    "aliases": aliases,
                    "aliases_text": aliases,
                    "popularity": row.get("count_languages", 0),
                    "popularity_log2p": math.log2((row.get("count_languages") or 0) + 1),
                    "vector": vector,
                    "wikidata_id": row['id'],
                }
            }


for cid, chunk in enumerate(chunking(rows, 8_000_000)):
    # Index the documents in bulk
    bulk(client, document_generator(chunk))
    print("Documents added successfully.", cid, total_keys)

# Define your documents to be indexed
rows = [
    {
        "id": "Q191",
        "title": "Estonia",
        "text": "Estonia is known for its cultural heritage",
        "aliases": ["Eesti"],
        "popularity": 1
    },
]
