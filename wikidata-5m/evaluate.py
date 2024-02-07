from typing import Optional

from pydantic import BaseModel, ConfigDict

from diskstorage import DiskSearch


class AliasSearch(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    cache: DiskSearch
    name: Optional[str] = None

    def search(self, row):
        mention_name, annotated_class, annotated_idx = row['name'], row['class'], row['id']
        return [
            {'wikidata_id': qid, 'name': mention_name}
            for qid in self.cache.read(mention_name) or []
        ]

    @property
    def name(self):
        return self.name or self.cache.__class__.__name__


if __name__ == '__main__':
    from search_dataset import SEARCH_DATASET, evaluate
    import pandas as pd

    search_limit = 10
    dataset = SEARCH_DATASET[:]
    data = []
    dataset_aliases = DiskSearch('data/wikidata-5m-dataset.aliases.cache')

    data += evaluate(dataset, AliasSearch(cache=dataset_aliases, name='wikidata-5m-alias'), search_limit)
    data = pd.DataFrame.from_records(data)
    print(data.groupby('source').mean(numeric_only=True).reset_index().round(2))

    print(dataset_aliases['Donald Trump'])
    print(dataset_aliases['Estonia'])
