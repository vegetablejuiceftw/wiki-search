from opensearchpy import OpenSearch

from diskstorage import DiskSearch
from noun_phases import get_phrases
from search_dataset.base import BaseSearch
from utils.embedding import load_st1
from sentence_transformers import util
import numpy as np


client = OpenSearch(
    hosts=["localhost"],
    http_auth=("admin", "7Tr0ngP@ssw0rdwget"),
    scheme="http",
    port=9200,
)
index_name = "search_index_v7_emb_lite"
dataset = DiskSearch(f'data/wikidata-v3.cache')


# qid, mention_name, mention_llm = 'Q728', 'arms', 'military weapons and equipment used for warfare and combat'
# qid, mention_name, mention_llm = 'Q528974', 'Baruto', 'A European-born professional sumo wrestler who achieved high-ranking titles.'
qid, mention_name, mention_llm = 'Q1151299', 'rag doll', 'A movable figure used in gaming battles.'
# qid, mention_name, mention_llm = 'Q536118', 'seizure', 'The legal seizure of funds or assets during a criminal investigation.'
# qid, mention_name, mention_llm = 'Q63437015', 'king', 'A powerful and prominent character in One Punch'
# qid, mention_name, mention_llm = 'Q22909116', 'Prime', "The period of optimal performance and understanding in one's career."
# qid, mention_name, mention_llm = 'Q5339301', 'squash ', 'A type of plant that is harvested'
# qid, mention_name, mention_llm = 'Q33602', 'pandas', 'A primarily plant-eating member of the order Carnivora.'

text_candidate = mention_name + ", " + mention_llm[0].lower() + mention_llm[1:].strip()
print(text_candidate)

test_vector = load_st1('cpu')[0]([text_candidate])[0].tolist()
example = dataset[qid]
print(example)

alises = example['aliases']


N = 9999
search_query = {
    "query": {
        "knn": {
            "vector": {
                "vector": test_vector,
                "k": N,
                "filter": {
                    "dis_max": {
                        "queries": [
                            {"match": {"title": {"query": p.lower()}}}
                            for p in example['aliases']
                        ],
                    },
                },
            },
        },
    },
}
# search_query = {
#     "query": {
#         "dis_max": {
#             "queries": [
#                 {"match": {"title": {"query": p.lower()}}}
#                 for p in example['aliases']
#             ],
#         },
#     },
# }

# ANY ALIAS
# search_query = {
#   "query": {
#     "script_score": {
#       "query": {
#         "dis_max": {
#             "queries": [
#                 {"match": {"title": {"query": p.lower()}}}
#                 for p in example['aliases']
#             ],
#         },
#       },
#       "script": {
#         "source": "cosineSimilarity(params.query_vector, doc['vector']) + 1.0",
#         "params": {
#           "query_vector": test_vector,
#         }
#       }
#     }
#   },
# }

# ONLY THE CURRENT ITEM IS SEARCHED
# search_query = {
#   "query": {
#     "script_score": {
#       "query": {"match": {"wikidata_id": qid}},
#       "script": {
#         "source": "cosineSimilarity(params.query_vector, doc['vector']) + 1.0",
#         "params": {"query_vector": test_vector,}
#       }
#     }
#   },
# }

# VECTOR ONLY
# search_query = {
#   "query": {
#     "script_score": {
#       "query": {"match_all": {}},
#       "script": {
#         "source": "cosineSimilarity(params.query_vector, doc['vector']) + 1.0",
#         "params": {"query_vector": test_vector,}
#       },
#     },
#   },
# }

#
# Search
#
response = client.search(index=index_name, body=search_query, size=N, timeout=30)
print("\nhits", response["hits"]["total"])
hits = response["hits"]["hits"]
for i, hit in enumerate(hits):
    sim = util.dot_score(test_vector, hit["_source"]['vector'])[0].item()
    if hit["_source"]["wikidata_id"] == qid or i < 3:
        correct = "###" if hit["_source"]["wikidata_id"] == qid else "   "
        print(f"{correct} {i}-index {sim:.2f}-sim, {hit['_score']:.2f}-score", hit["_source"]["wikidata_id"], hit["_source"]['text'][:64])


def process(c):
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
        yield t    

process(example)


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

for key in key_index[qid]:
    vector = embeddings_fp[int(key)].tolist()
    sim = util.cos_sim(test_vector, vector)[0].item()
    print(round(sim, 3), key)

emb = load_st1('cpu')[0]
for c in process(example):
    sim = util.dot_score(
        emb(text_candidate), 
        emb(c))[0].item()
    print(round(sim, 3), c)
