from itertools import chain
from typing import Optional

from pydantic import BaseModel, ConfigDict

from diskstorage import DiskSearch, AliasSearch
from noun_phases import get_phrases
from utils.fuzzy import FuzzySearch


#               source       method  candidates  rank  found
# 0  wikidata-5m-alias  AliasSearch        1.45  0.28   0.35
# 1      wikidata-full  AliasSearch       26.90  6.56   0.84
# 2     wikidata-fuzzy  FuzzySearch       66.36  6.33   0.90

#                  source       method  search_limit  candidates  rank  found
# 1      wikidata-v2-full  AliasSearch          32.0       26.44  6.39   0.82
# 2     wikidata-v2-fuzzy  FuzzySearch          32.0       68.94  5.98   0.88
#            source       method  search_limit  candidates  rank  found
# 0  wikidata-fuzzy-80  FuzzySearch          16.0       66.67  5.98   0.88

#                    source       method  search_limit  candidates  rank  found
# 0          wikidata-fuzzy90  FuzzySearch          16.0       33.77  6.12   0.85
# 1  wikidata-fuzzy-phrases90  FuzzySearch          16.0       36.60  6.94   0.88
#                    source       method  search_limit  candidates  rank  found
# 0          wikidata-fuzzy85  FuzzySearch          16.0       55.75  6.07   0.86
# 1  wikidata-fuzzy-phrases85  FuzzySearch          16.0       63.89  7.76   0.89

if __name__ == '__main__':
    from search_dataset import SEARCH_DATASET, evaluate
    import pandas as pd
    import numpy as np

    # cache = DiskSearch('data/wikidata.aliases-lc.cache')
    cache = DiskSearch('data/wikidata-v2.aliases-lc.cache')

    search_limit = 16
    dataset = SEARCH_DATASET[:]
    data = []
    # data += evaluate(dataset,
    #                  AliasSearch(cache=DiskSearch('data/wikidata-5m-dataset.aliases.cache'), name='wikidata-5m-alias'),
    #                  search_limit)
    # data += evaluate(dataset, AliasSearch(cache=cache, name='wikidata-full'),
    #                  search_limit)

    data += evaluate(dataset, FuzzySearch(cache=cache, keys=tuple(cache.keys()), name='wikidata-fuzzy', search_limit=search_limit),
                     search_limit)
    # data += evaluate(dataset, FuzzySearch(cache=cache, keys=tuple(cache.keys()), name='wikidata-fuzzy-phrases', search_limit=search_limit, phrases=True),
    #                  search_limit)
                     
    data = pd.DataFrame.from_records(data)
    print(data[~data.found]['name'].to_list())
    print(data.groupby(['source', 'method']).mean(numeric_only=True).sort_values("found").reset_index().round(2))

    dataset_5m = DiskSearch('data/wikidata-5m-dataset.cache')
    containment = [bool(dataset_5m[qid]) for row in dataset for qid in row["id"].split(';') ]
    print(np.mean(containment).round(2))

    dataset_5m = DiskSearch('data/wikidata.cache')
    containment = [bool(dataset_5m[qid]) for row in dataset for qid in row["id"].split(';') ]
    print(np.mean(containment).round(2))

    containment = [row['id'] for row in dataset for qid in row["id"].split(';') if not bool(dataset_5m[qid])]
    print(containment)

