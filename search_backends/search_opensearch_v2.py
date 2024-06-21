from opensearchpy import OpenSearch

from diskstorage import DiskSearch
from noun_phases import get_phrases
from search_dataset.base import BaseSearch
from utils.embedding import load_st1


client = OpenSearch(
    hosts=["localhost"],
    http_auth=("admin", "7Tr0ngP@ssw0rdwget"),
    scheme="http",
    port=9200,
)
index_name = "search_index_v7_emb_lite"

dataset = DiskSearch(f'data/wikidata-v3.cache')

# example = dataset['Q191']
# print(example)
# keyword = example['labels'][0].lower()
# alises = example['aliases']
# print(keyword)

# search_query = {
#     "query": {
#         "dis_max": {
#             "queries": [
#                 {"match": {"aliases": p.lower()}}
#                 for p in alises
#             ],
#         }
#     }
# }

# response = client.search(index=index_name, body=search_query, size=8, timeout=30)
# print(response["hits"]["total"])

# vector = None
# hits = response["hits"]["hits"]
# for i, hit in enumerate(hits):
#     print(i, round(hit["_score"], 1), hit["_id"], hit["_source"]['title'], hit["_source"]['text'], "\n\t",
#           hit["_source"]['aliases'])
#     if hit["_id"] == example['id']:
#         vector = hit["_source"]['vector']
#         # break
#         print("# # # #")


# # from utils.embedding import load_st1

# # vector = load_st1(device="cpu")[0]([example['text']])[0].tolist()


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
                                            {"match": {"aliases": {"query": name.lower(), "boost": mapping.get("aliases-a", 0.35)}}},
                                            {"match": {"aliases": {"query": name.lower(), "fuzziness": "AUTO","boost": mapping.get("aliases-b", 0.86),}}},
                                            {"match": {"aliases": {"query": name.lower(), "boost": mapping.get("aliases-c", 0.18),"analyzer": "english",}}},
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

search_query_template = lambda names, mapping, boost=0.07: {
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
            "boost": boost,
        },
    },
}


# search_query = {
#     "query": {
#         "dis_max": {
#             "queries": [
#                 {"match": {"aliases": p.lower()}}
#                 for p in alises
#             ],
#         }
#     }
# }

# search_query = {
#     "query": {
#         "bool": {
#             "should": [
#                 {
#                     "function_score": {
#                         "query": {
#                             "bool": {
#                                 "should": [
#                                     {"match": {"aliases": {"query": name.lower(), "boost": 0.35}}},
#                                     {"match": {"aliases": {"query": name.lower(), "boost": 0.18, "analyzer": "english"}}},
#                                     {"term": {"aliases": {"value": name.lower(), "boost": 0.93}}},
#                                     {"match": {"title": {"query": name,"boost": 0.46, "analyzer": "english"}}},
#                                     {"match": {"title": {"query": name,"fuzziness": "AUTO","boost": 0.45, }}},
#                                 ]
#                             }
#                         },
#                         "score_mode": "sum"  # Combine scores from multiple match statements
#                     }
#                 }
#                 for name in alises
#             ],
#         },
#     },
# }

# # search_query = {
# #     "query": {
# #         "knn": {
# #             "vector": {
# #                 "vector": vector,
# #                 "k": 4,
# #                 "filter": {
# #                     "dis_max": {
# #                         "queries": [
# #                             {"match": {"aliases": p.lower()}}
# #                             for p in alises
# #                         ],
# #                     }
# #                 },
# #             },
# #         }
# #     }
# # }

# response = client.search(index=index_name, body=search_query, size=4, timeout=30)
# print(response["hits"]["total"])
# hits = response["hits"]["hits"]
# for i, hit in enumerate(hits):
#     print(i, round(hit["_score"], 1), hit["_id"], hit["_source"]['title'], hit["_source"]['text'])
#     if hit["_id"] == example['id']:
#         # break

#         print("# # # #")

# # exit()

# client: OpenSearch
# # Perform a hybrid search
# search_query = {
#     "query": {
#         "hybrid": {
#             "queries": [
#                 {
#                     "dis_max": {
#                         "queries": [
#                             {"match": {"aliases": p.lower()}}
#                             for p in alises
#                         ],
#                     },
#                 },
#                 {
#                     "knn": {
#                         "vector": {
#                             "vector": vector,
#                             "k": 16,
#                         }
#                     }
#                 }
#             ]
#         }
#     }
# }

# # search_query = {
# #     "query": {
# #         "bool": {
# #             "must": [
# #                 {
# #                     "match": {
# #                         "aliases": {
# #                             "query": keyword,
# #                             "boost": 0.01,
# #                         }
# #                     }
# #                 }
# #             ],
# #             "should": [
# #                 {
# #                     "knn": {
# #                         "vector": {
# #                             "vector": vector,
# #                             "k": 32
# #                         }
# #                     }
# #                 }
# #             ]
# #         }
# #     }
# # }
# #
# search_query = {
#     "query": {
#         "bool": {
#             "should": [
#                 {
#                     "dis_max": {
#                         "queries": [
#                             {"match": {"aliases": {"query": p.lower(), "boost": 0.1}}}
#                             for p in alises
#                         ],
#                     },
#                 },
#                 {
#                     "knn": {
#                         "vector": {
#                             "vector": vector,
#                             "k": 32
#                         }
#                     }
#                 }
#             ]
#         }
#     }
# }

# response = client.search(index=index_name, body=search_query, timeout=90, size=32)
# # print(response)
# print(response["hits"]["total"])
# for i, hit in enumerate(response["hits"]["hits"]):
#     print(i, round(hit["_score"], 1), hit["_id"],
#           "\n\t", [hit["_source"]['title'], hit["_source"]['text']],
#           #
#           # hit["_source"]['aliases']
#           )
#     if hit["_id"] == example['id']:
#         # break
#         print()
        
# exit()


"""
For the following text, extract relevant keywords and disambiguate them with dictionary like definitions. 

Please pay attention to the whole text as the keywords in this text are confusing and easily mistaken.

<text>return of the Hero Let’s start watching Let’s hear about King, the strongest hero on earth Really? Hey This scene is… When was it? I yeah, I remember Genos isn't in this scene Sonic is here to look for Saitama In short, the Hero Association lacks manpower, so they're asking the criminals for</text>

First list all keywords, then explain what they have in common, then disambiguate. Answer in json.
"""



class HybridSearch(BaseSearch):
    searcher: object

    def search(self, row):
        mention_name, annotated_text, mention_llm = row['name'], row['text'], row['llm']
        results = []
        phrases = get_phrases(mention_name, annotated_text)
        # phrases = [mention_name]

        text_candidate = mention_name + ", " + mention_llm[0].lower() + mention_llm[1:].strip()
        vector = load_st1('cpu')[0]([text_candidate])[0].tolist()

        # Test 1 : 0.74 / 1.71
        # search_query = {
        #     "query": {
        #         "knn": {
        #             "vector": {
        #                 "vector": vector,
        #                 "k": self.search_limit,
        #                 # "filter": {
        #                 #     "dis_max": {
        #                 #         "queries": [
        #                 #             {"match": {"aliases": {"query": p.lower(), "boost": 0.1}}}
        #                 #             for p in phrases
        #                 #         ],
        #                 #     },
        #                 # },
        #                 # "filter": {
        #                 #     "bool": {
        #                 #         "must": [
        #                 #             {
        #                 #                 "dis_max": {
        #                 #                     "queries": [
        #                 #                         {"match": {"aliases": {"query": p.lower()}}}
        #                 #                         for p in phrases
        #                 #                     ],
        #                 #                 },
        #                 #             },
        #                 #         ],
        #                 #         # "minimum_should_match": "1",
        #                 #     }
        #                 # },
        #             },
        #         }
        #     }
        # }
        # TEST 2 :  0.74 / 2.07
        # search_query = {
        #     "query": {
        #         "bool": {
        #             "should": [
        #                 {
        #                     "dis_max": {
        #                         "queries": [
        #                             {"match": {"aliases": {"query": p.lower(), "boost": 0.1}}}
        #                             for p in phrases
        #                         ],
        #                     },
        #                 },
        #                 {
        #                     "knn": {
        #                         "vector": {
        #                             "vector": vector,
        #                             "k": 32
        #                         }
        #                     }
        #                 }
        #             ]
        #         }
        #     }
        # }

        # search_query = {
        #     "query": {
        #         "bool": {
        #             "must": [
        #                 {
        #                     "bool": {
        #                         "should": [
        #                             {
        #                                 "match": {
        #                                     "aliases": {
        #                                         "query": p.lower(),
        #                                         "boost": 0.1,
        #                                     }
        #                                 }
        #                             }
        #                             for p in phrases
        #                         ],
        #                         "minimum_should_match": 1
        #                     }
        #                 }
        #             ],
        #             "should": [
        #                 {
        #                     "knn": {
        #                         "vector": {
        #                             "vector": vector,
        #                             "k": self.search_limit,
        #                         }
        #                     }
        #                 }
        #             ]
        #         }
        #     }
        # }

        # TEST 3 : 0.68 / 13.23
        # search_query = {
        #     "query": {
        #         "hybrid": {
        #             "queries": [
        #                 # search_query_template(phrases, {})['query'],
        #                 # {
        #                 #     "dis_max": {
        #                 #         "queries": [
        #                 #             {"match": {"aliases": {"query": p.lower(), "boost": 0.1}}}
        #                 #             for p in phrases
        #                 #         ],
        #                 #     }
        #                 # },
        #                 # {
        #                 #     "knn": {
        #                 #         "vector": {
        #                 #             "vector": vector,
        #                 #             "k": self.search_limit,
        #                 #         }
        #                 #     }
        #                 # }
        #             ]
        #         }
        #     }
        # }

        # 84
        # search_query = {
        #     "query": {
        #         "function_score": {
        #             "query": {
        #                 "bool": {
        #                     "should": [
        #                         {
        #                             "function_score": {
        #                                 "query": {
        #                                     "bool": {
        #                                         "should": [
        #                                             {"term": {"aliases": {"value": name.lower(), "boost": 0.9}}},
        #                                             {"match": {"title": {"query": name, "fuzziness": "AUTO","boost": 0.10, }}},
        #                                         ]
        #                                     }
        #                                 },
        #                                 "score_mode": "sum"  # Combine scores from multiple match statements
        #                             }
        #                         }
        #                         for name in phrases
        #                     ],
        #                 },
        #             },
        #             "score_mode":  "max",  # Focus on the maximum score among sub-queries
        #         }
        #     }
        # }
        search_query = {
            "query": {
                "bool": {
                    "should": [
                        search_query_template(phrases, {}, boost=0.01)["query"],  # 0.01, 0.02
                        {
                            "dis_max": {
                                "queries": [
                                    {"match": {"aliases": {"query": p.lower(), "boost": 0.01}}}
                                    for p in phrases
                                ],
                            }
                        },
                        # {
                        #     "dis_max": {
                        #         "queries": [
                        #             {"match": {"title": {"query": p, "boost": 0.01}}}
                        #             for p in phrases
                        #         ],
                        #     },
                        # },
                        # {
                        #     "dis_max": {
                        #         "queries": [
                        #             {"match": {"title": {"query": p.lower(), "fuzziness": "AUTO","boost": 0.005, }}}
                        #             for p in phrases
                        #         ],
                        #     }
                        # },
                         {
                            "dis_max": {
                                "queries": [
                                    {"match": {"aliases.text": {"query": p.lower(), "fuzziness": "AUTO", "boost": 0.02, }}}
                                    for p in phrases
                                ],
                            }
                        },
                        # {
                        #     "knn": {
                        #         "vector": {
                        #             "vector": vector,
                        #             "k": self.search_limit,
                        #         }
                        #     }
                        # },

                        {
                            "script_score": {
                                "query": {
                                    "match_all": {}
                                },
                                "script": {
                                    "source": "doc['popularity_log2p'].value * 0.02",
                                    "params": {
                                        "query_vector": vector,
                                    }
                                }
                            },
                        },
                        {
                            "script_score": {
                                "query": {
                                    "match_all": {}
                                },
                                "script": {
                                    "source": "Math.max(cosineSimilarity(params.query_vector, doc['vector']), 0.0)",
                                    "params": {
                                        "query_vector": vector,
                                    }
                                }
                            },
                        }
                    ],
                },
            }
        }
        # r 5.6 f 89
        # search_query = search_query_template(phrases, {})

        # search_query = {
        #     "query": {
        #         "script_score": {
        #         "query": {
        #             "match_all": {}
        #         },
        #         "script": {
        #             "source": "cosineSimilarity(params.query_vector, doc['vector']) + 1.0",
        #             "params": {
        #             "query_vector": vector,
        #             }
        #         }
        #         }
        #     }
        # }

        response = client.search(index=index_name, body=search_query, timeout=60, size=self.search_limit, _source_includes=['wikidata_id', 'title','aliases'])
        hits = response["hits"]["hits"]
        for i, hit in enumerate(hits):
            if hit["_score"] > 0.32 or 1:
                # print(i, round(hit["_score"], 3), hit["_source"]['title'], hit["_source"]['aliases'])
                results.append({
                    "wikidata_id": hit["_source"]["wikidata_id"],
                    "score": hit["_score"],
                    "rank": i,
                })
        seen = set()
        out = []
        for r in results:
            if r['wikidata_id'] not in seen:
                out.append(r)
                seen.add(r['wikidata_id'])
        results = out        
        # print([mention_name, mention_llm])
        # print(phrases)        
        return results


from search_dataset import SEARCH_DATASET, evaluate
import pandas as pd
from tqdm.auto import tqdm
import json

# model = HybridSearch(
#     searcher=client,
#     search_limit=4096,
#     name='wikidata-hybrid',
# )
# results = {}
# for row in tqdm(SEARCH_DATASET):
#     results[row['id']] = row
#     for result in model.search(row): 
#         results[result['wikidata_id']] = result
# with open("top_4k.json", "w") as f:
#     json.dump(results, f)
# print("DUMPED", len(results))

search_limit = 50
# search_limit = 9999
dataset = SEARCH_DATASET#[::7]
# dataset = [d for d in dataset if d['id'] in ['Q63437015', 'Q55418044', 'Q5339301', 'Q528974', 'Q33602']]
data = []
data += evaluate(
    dataset,
    HybridSearch(
        searcher=client,
        search_limit=search_limit,
        name='wikidata-hybrid',
    ),
    search_limit,
)

data = pd.DataFrame.from_records(data)
print(data[~data.found]['id'].to_list())
print(data.groupby(['source', "method"]).mean(numeric_only=True).reset_index().round(2))


# TODO reduce db size to relevant mistakes and resultst
#             source        method  search_limit  candidates  rank  found   score
# 0  wikidata-hybrid  HybridSearch          96.0        96.0   7.0   0.95  117.63
