from typing import Optional, List, Callable

from pydantic import BaseModel, ConfigDict
from whoosh.index import open_dir

from diskstorage import DiskSearch, AliasSearch
from search_index.helpers import SplitTokenizer
from utils.embedding import load_use4, load_st1
from noun_phases import get_phrases, get_noun_phrases

from whoosh.qparser import QueryParser
from whoosh.query import BooleanQuery, Or, FuzzyTerm
from whoosh.searching import Searcher


class EmbeddingSearch(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    index_cache: DiskSearch
    index_search: object
    emb_function: object
    searcher_func: object

    name: Optional[str] = None
    search_limit: int = 32

    def search(self, row):
        mention_name, annotated_text, mention_class = row['name'], row['text'], row['class']
        mention_llm = row['llm']

        text = [
            # mention_name + ", " + mention_llm.split(":")[-1].strip(),
            # mention_llm.split(":")[-1].strip(),
            mention_llm,
            # *mention_llm.split(", "),
            annotated_text,
            f"{mention_name}, {mention_class}",
            # ", ".join(get_noun_phrases(annotated_text)),
        ]
        text = [s.replace("(", "").replace(")", "") for s in text]
        # print(text)

        emb = self.emb_function()[0](text)
        D, I = self.index_search.search(emb, self.search_limit)

        results = {}
        for i in range(len(text)):
            for j in range(self.search_limit):
                neighbor_index = I[i, j]
                distance = D[i, j]
                r_key = self.index_cache[str(neighbor_index)]
                results[r_key] = {
                    'wikidata_id': r_key,
                    'distance': distance,
                    'name': mention_name
                }

        output = []
        phrases = {p: 1 for p in get_phrases(mention_name, annotated_text) + mention_llm.split(",")}.keys()
        phrases = [p.strip() for p in phrases]
        print(phrases)
        output += self.searcher_func(phrases, self.search_limit)
        output += sorted(results.values(), key=lambda d: d['distance'])
        output = list({d['wikidata_id']: d for d in output}.values())
        return output

    @property
    def method(self):
        return self.__class__.__name__

    @property
    def source(self):
        return self.name or self.cache.__class__.__name__


def whoosh_search(
        phrases: List[str],
        search_limit: int = 32,
):
    output = []
    boosts = {
        "title": 0.5,
        "label": 5.0,
        "alias": 1.0,
        # "alias_ngram": 0.10,
        # "description": 0.001,
    }

    for p in phrases:
        query = Or([
            QueryParser(field, searcher.schema).parse(f"'{p}'").with_boost(boost)
            for field, boost in boosts.items()
        ])
        for hit in searcher.search(query, limit=search_limit):
            output.append({
                "wikidata_id": hit['id'],
                **hit
            })
    return output


def rapidfuzz_search(
        phrases: List[str],
        search_limit: int = 32,
):
    from rapidfuzz.process import extract
    from rapidfuzz.fuzz import QRatio

    output = []
    for p in phrases:
        hits = extract(p, alias_keys, scorer=QRatio, score_cutoff=85, limit=search_limit)
        for hit in hits:
            output.append({
                "wikidata_id": hit[0],
            })
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
            })
    return output


def noop_search(
        phrases: List[str],
        search_limit: int = 32,
):
    return []


if __name__ == '__main__':
    from search_dataset import SEARCH_DATASET, evaluate
    import pandas as pd

    import faiss

    index_dir = "data/whoosh-v3.lite/"
    assert SplitTokenizer
    searcher: Searcher = open_dir(index_dir).searcher()

    search_limit = 128
    dataset = SEARCH_DATASET[:]
    data = []

    alias_index = DiskSearch('data/wikidata-v3.aliases-lc.cache')
    # alias_keys = tuple(alias_index.keys())
    index_cache = DiskSearch('data/wikidata.index.v4.cache')

    vector_cache = 'data/test.v4.ann'
    emb_function = load_st1
    index = faiss.read_index(vector_cache)

    for searcher_func in [
        noop_search,
        alias_search,
        whoosh_search,
        # rapidfuzz_search,
    ]:
        data += evaluate(
            dataset,
            EmbeddingSearch(index_search=index, index_cache=index_cache, search_limit=search_limit,
                            emb_function=emb_function, searcher_func=searcher_func, name=searcher_func.__name__),
            search_limit,
        )

        df = pd.DataFrame.from_records(data)
        print(df.groupby(['source', 'method']).mean(numeric_only=True).sort_values("found").reset_index().round(2))

#           source           method  search_limit  candidates  distance    rank  found  language_count
# 0    noop_search  EmbeddingSearch         128.0      355.17      0.80  127.15   0.25             NaN
# 1   alias_search  EmbeddingSearch         128.0      433.60      0.81   14.74   0.91             NaN
# 2  whoosh_search  EmbeddingSearch         128.0      863.20      0.80   47.52   0.95            56.9