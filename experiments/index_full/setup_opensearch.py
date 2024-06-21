import re
import time

from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import os
import shutil
from typing import List
import numpy as np
import math

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
index_name = "full_index_v6"

# for name in [
#     index_name,
# ]:
#     try:
#         client.indices.delete(index=name)
#         print("DELETED", index_name)
#     except Exception as e:
#         print("fail", e)

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
watermark_settings = {
    "persistent": {
        "indices.query.bool.max_clause_count": 4096,
        "cluster.routing.allocation.disk.threshold_enabled": True,
        "cluster.routing.allocation.disk.watermark.low": "99%",
        "cluster.routing.allocation.disk.watermark.high": "99%",
        "cluster.routing.allocation.disk.watermark.flood_stage": "99%"
    }
}

# client.cluster.put_settings(body=watermark_settings)
# client.indices.create(index=index_name, body=settings_body)
# client.indices.put_mapping(index=index_name, body=mapping_body)

dataset = DiskSearch(f'data/wikidata-v3.cache')
print(dataset['Q191'])

embedding_cache = 'data/embeddings/v6/wikidata.emb.cache'
index_cache = 'data/embeddings/v6/wikidata.index.cache'
inverse_cache = 'data/embeddings/v6/wikidata.inverse.cache'

total_rows = 27_420_075  # len(tuple(dataset.keys()))
embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='r', shape=(total_rows * 2, 384))
key_index = DiskSearch(inverse_cache)

# emb_index = dict(DiskSearch(index_cache).iter())
# # total_keys = len(emb_index)
# key_index = {}
# for k, q in tqdm(emb_index.items(), desc='inverse map'):
#     if not key_index:print([k, q])
#     arr = key_index.get(q)
#     if arr is None:
#         key_index[q] = [k]
#     else:
#         arr.append(k)
#
# inverse_index.write(key_index.items())

rows = (
    row
    for key, row in tqdm(dataset.iter(), total=total_rows, smoothing=0.01, desc="docs")
    if (
        row['text']
        and key.startswith('Q')
        # and row.get("count_languages", 0) > 1
        # and any("est" in a.lower() for a in row['labels'])
        # and row.get("count_languages", 0) > 0 # and any(k in a.lower() for a in row['aliases'] for k in ['est',]) and row.get("count_languages", 0) > 5
    )
)

#
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


for cid, chunk in enumerate(chunking(rows, 10000)):
    if cid < 3:
        continue
    while True:
        try:
            # Index the documents in bulk
            bulk(client, document_generator(chunk))
            break
        except Exception as e:
            print(e)
            time.sleep(100)
        finally:
            print("Documents added successfully.", cid)




# from opensearchpy import OpenSearch
#
# client = OpenSearch(
#     hosts=["localhost"],
#     http_auth=("admin", "7Tr0ngP@ssw0rdwget"),  # Change to your admin credentials
#     scheme="http",
#     port=9200,
# )
# index_name = "full_index_v6"
#
# mapping_body = {
#     "properties": {
#         "wikidata_id": {"type": "keyword"},
#         "popularity": {"type": "float"},
#         "aliases": {
#             "type": "keyword",
#             "fields": {
#                 "text": {"type": "text"}
#             }
#         },
#     }
# }
# client.indices.put_mapping(index=index_name, body=mapping_body)
#
# # Define your documents to be indexed
# rows = [
#     {
#         "id": "Q191",
#         "title": "Estonia",
#         "text": "Estonia is known for its cultural heritage",
#         "aliases": ["Eesti"],
#         "popularity": 1
#     },
# ]