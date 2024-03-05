from typing import Optional

from pydantic import BaseModel, ConfigDict

from diskstorage import DiskSearch, AliasSearch
from utils.fuzzy import FuzzySearch
from utils.embedding import load_use4, load_st1
from noun_phases import get_phrases, get_noun_phrases


class EmbeddingSearch(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    index_cache: DiskSearch
    index_search: object
    model_loader: object

    name: Optional[str] = None
    search_limit: int = 32

    def search(self, row):
        mention_name, annotated_text, annotated_idx = row['name'], row['text'], row['id']
        mention_class = row['class']
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

        emb = self.model_loader()[0](text)
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
        output += alias_search.search(row)
        output += alias_search.search_phrases([mention_name] + [p.strip() for p in mention_llm.split(",")])
        output += sorted(results.values(), key=lambda d: d['distance'])
        # output = alias_search.search(row)  + alias_search.search_phrases([p.strip() for p in mention_llm.split(",")]) + results
        output = list({d['wikidata_id']: d for d in output}.values())
        return output
        

    @property
    def method(self):
        return self.__class__.__name__

    @property
    def source(self):
        return self.name or self.cache.__class__.__name__


if __name__ == '__main__':
    from search_dataset import SEARCH_DATASET, evaluate
    import pandas as pd

    import numpy as np
    import faiss

    search_limit = 8
    dataset = SEARCH_DATASET[:]
    data = []

    cache = DiskSearch('data/wikidata.aliases-lc.cache')
    index_cache = DiskSearch('data/wikidata.index.v4.cache')

    vector_cache = 'data/test.v4.ann'
    model_loader = load_st1
    index = faiss.read_index(vector_cache)

    alias_search = AliasSearch(cache=cache, name='wikidata-full')
    data += evaluate(
        dataset,
        EmbeddingSearch(index_search=index, index_cache=index_cache, search_limit=search_limit, model_loader=model_loader, name='wikidata-full-emb-faiss'),
        search_limit,
    )    

    alias_search = FuzzySearch(cache=cache, keys=tuple(cache.keys()), name='wikidata-fuzzy')
    data += evaluate(
        dataset,
        EmbeddingSearch(index_search=index, index_cache=index_cache, search_limit=search_limit, model_loader=model_loader, name='wikidata-full-emb-faiss'),
        search_limit,
    )

    data = pd.DataFrame.from_records(data)
    print(data[~data.found][['id', 'name', 'class', 'rank', 'text', 'llm']])
    print(data.groupby(['source', 'method']).mean(numeric_only=True).sort_values("found").reset_index().round(2))

