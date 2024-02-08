from itertools import chain
from typing import Optional

from pydantic import BaseModel, ConfigDict

from diskstorage import DiskSearch
from noun_phases import get_phrases


class AliasSearch(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    cache: DiskSearch
    name: Optional[str] = None

    def search(self, row):
        mention_name, annotated_text, annotated_idx = row['name'], row['text'], row['id']
        phrases = get_phrases(mention_name, annotated_text)
        return list({
                        qid: {'wikidata_id': qid, 'name': mention_name}
                        for qid in chain(*[
                self.cache.read(p.lower()) or []
                for p in phrases
            ])
                    }.values())

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

    search_limit = 10
    dataset = SEARCH_DATASET[:]
    data = []
    data += evaluate(dataset,
                     AliasSearch(cache=DiskSearch('data/wikidata-5m-dataset.aliases.cache'), name='wikidata-5m-alias'),
                     search_limit)
    data += evaluate(dataset, AliasSearch(cache=DiskSearch('data/wikidata.aliases-lc.cache'), name='wikidata-full'),
                     search_limit)
    data = pd.DataFrame.from_records(data)
    print(data.groupby('source').mean(numeric_only=True).reset_index().round(2))

    dataset_5m = DiskSearch('data/wikidata-5m-dataset.cache')
    containment = [bool(dataset_5m[row['id']]) for row in dataset]
    print(np.mean(containment).round(2))

    dataset_5m = DiskSearch('data/wikidata.cache')
    containment = [bool(dataset_5m[row['id']]) for row in dataset]
    print(np.mean(containment).round(2))
