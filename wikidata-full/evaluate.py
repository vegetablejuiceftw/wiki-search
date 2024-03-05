from diskstorage import DiskSearch
from annoy import AnnoyIndex

from search_dataset.base import BaseSearch
from utils.embedding import load_use4


class EmbeddingSearch(BaseSearch):

    index_cache: DiskSearch
    index_search: AnnoyIndex

    def search(self, row):
        mention_name, annotated_text, annotated_idx = row['name'], row['text'], row['id']
        # annotated_text = mention_name + ", " + dataset_wd[annotated_idx]['text']
        emb = load_use4()[0]([annotated_text])[0]

        results = [
            {'wikidata_id': self.index_cache[str(i)], 'name': mention_name}
            for i in u.get_nns_by_vector(emb, self.search_limit)
        ]
        return results


if __name__ == '__main__':
    from search_dataset import SEARCH_DATASET, evaluate
    import pandas as pd

    # dataset_wd = DiskSearch('data/wikidata.cache')

    index_cache = 'data/wikidata.index.v2.cache'
    vector_cache = 'data/test.v2.ann'

    u = AnnoyIndex(512, 'dot')
    u.load(vector_cache)

    search_limit = 128
    dataset = SEARCH_DATASET[:]
    data = []
    data += evaluate(
        dataset,
        EmbeddingSearch(index_search=u, index_cache=DiskSearch(index_cache), search_limit=search_limit, name='wikidata-full-emb'),
        search_limit,
    )

    data = pd.DataFrame.from_records(data)
    print(data.groupby(['source', "method"]).mean(numeric_only=True).reset_index().round(2))
