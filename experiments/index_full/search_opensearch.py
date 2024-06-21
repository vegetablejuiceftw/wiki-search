from opensearchpy import OpenSearch

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
                                        {"match": {
                                            "aliases_text": {"query": name.lower(), "boost": mapping.get("aliases-a", 0.35)}}},
                                        {"match": {"aliases_text": {"query": name.lower(), "fuzziness": "AUTO",
                                                               "boost": mapping.get("aliases-b", 0.86), }}},
                                        {"match": {"aliases_text": {"query": name.lower(), "boost": mapping.get("aliases-c", 0.18),
                                                               "analyzer": "english", }}},
                                        # {"match": {"title": {"query": name, "boost": mapping.get("title-a", 0.07)}}},
                                        {"match": {"title": {"query": name, "boost": mapping.get("title-b", 0.46),
                                                             "analyzer": "english", }}},
                                        {"match": {"title": {"query": name, "fuzziness": "AUTO",
                                                             "boost": mapping.get("title-c", 0.45), }}},
                                        # exact lookups
                                        {"term": {
                                            "title.keyword": {"value": name, "boost": mapping.get("title-kw", 0.00)}}},
                                        {"term": {"aliases": {"value": name.lower(),
                                                              "boost": mapping.get("aliases-kw", 0.93)}}},
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
                        "modifier": mapping.get("factor-modifier", "log2p"),
                        # Apply logarithmic function to popularity score
                        "factor": mapping.get("factor-a", 6.6),  # Adjust the boosting factor based on your requirements
                    }
                }
            ],
            "score_mode": mapping.get("factor-mode", "sum"),
        },
    },
}


from noun_phases import get_phrases
from search_dataset.base import BaseSearch


class OpenSearchSearch(BaseSearch):
    searcher: object
    field_boosts: dict

    def search(self, row):
        mention_name, annotated_text, mention_llm = row['name'], row['text'], row['llm']
        results = []
        phrases = get_phrases(mention_name, annotated_text)
        query = search_query_template(phrases, self.field_boosts)
        response = self.searcher.search(index=index_name, body=query, size=self.search_limit, _source=False)
        hits = response["hits"]["hits"]
        for i, hit in enumerate(hits):
            results.append({
                "wikidata_id": hit["_id"].split("-")[0],
                "score": hit["_score"],
                "rank": i,
            })

        results = list({d['wikidata_id']: d for d in results}.values())
        return results

client = OpenSearch(
    hosts=["localhost"],
    http_auth=("admin", "7Tr0ngP@ssw0rdwget"),
    scheme="http",
    port=9200,
)
index_name = "full_index_v6"

search_limit = 96
searcher = OpenSearchSearch(
        searcher=client,
        search_limit=search_limit,
        name='wikidata-open-search',
        # field_boosts={},
        # field_boosts={'aliases-a': 0.17907659116565466, 'aliases-b': 0.9800831882687093, 'aliases-c': 0.1901811335365765, 'title-a': 0.11766649750958846, 'title-b': 0.5354962306571437, 'title-c': 0.17202132009551674, 'title-kw': 0.4653912475965112, 'aliases-kw': 0.7919284147288651, 'factor-a': 0.743677932042815}
        field_boosts={'aliases-a': 0.6000000000000001, 'aliases-b': 0.6000000000000001, 'aliases-c': 0.45, 'title-a': 0.65, 'title-b': 0.30000000000000004, 'title-c': 0.35000000000000003, 'title-kw': 0.2, 'aliases-kw': 0.9, 'factor-a': 4.694751770347057}
    )


if __name__ == '__main__':
    from search_dataset import SEARCH_DATASET, evaluate
    import pandas as pd

    data = pd.DataFrame.from_records(
        evaluate(
            SEARCH_DATASET[::],
            searcher,
            search_limit,
        )
    )
    print(data.groupby(['source', "method"]).mean(numeric_only=True).reset_index().round(2))
