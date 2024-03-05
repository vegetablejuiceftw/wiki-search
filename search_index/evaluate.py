from whoosh import sorting
from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from whoosh.query import BooleanQuery, Or, FuzzyTerm
from whoosh.searching import Searcher

from noun_phases import get_phrases
from search_dataset.base import BaseSearch
from search_index.helpers import SplitTokenizer


class WhooshSearch(BaseSearch):
    searcher: Searcher

    def search(self, row):
        mention_name, annotated_text, annotated_idx = row['name'], row['text'], row['id']

        phrases = get_phrases(mention_name, annotated_text)

        boosts = {
            # "title": 0.5,
            # "label": 5.0,
            "alias": 1.0,
            "alias_ngram": 0.10,
            # "description": 0.001,
        }
        # queries = [
        #     QueryParser(field, self.searcher.schema).parse(f"'{p}'").with_boost(boost)
        #     for field, boost in boosts.keys()
        # ]
        #
        # query = Or(queries)

        # query_parser = QueryParser("alias", schema=self.searcher.schema)
        # query_label_parser = QueryParser("label", schema=self.searcher.schema)
        # title_query_parser = QueryParser("title", schema=self.searcher.schema)
        # # query = Or(list(
        # #     qp.parse(p)
        # #     for p in phrases
        # #     for qp in [
        # #         query_parser,
        # #         title_query_parser
        # #     ]
        # # ))
        #
        # # query = Or(
        # #     QueryParser("alias", schema=self.searcher.schema).parse(mention_name).with_boost(2.0),
        # #     FuzzyTerm("alias", mention_name, maxdist=2),
        # # )

        results = []
        facet = sorting.FieldFacet("language_count", reverse=True)
        for p in phrases:
            queries = [
                QueryParser(field, self.searcher.schema).parse(f"'{p}'").with_boost(boost)
                for field, boost in boosts.items()
            ]
            #
            query = Or(queries)
            # query = query_parser.parse(f"'{p}'") | query_label_parser.parse(f"'{p}'").with_boost(5.0) | title_query_parser.parse(f"'{p}'").with_boost(0.5)
            # print(query)
            for hit in self.searcher.search(query, limit=self.search_limit, scored=True):
                results.append({
                    "wikidata_id": hit['id'],
                    "language_count": hit['language_count'],
                    **hit
                })
        # query = FuzzyTerm("alias", mention_name, maxdist=3)
        # for hit in self.searcher.search(query, limit=self.search_limit):
        #     results.append({
        #         "wikidata_id": hit['id'],
        #         **hit
        #     })

        # for hit in self.searcher.search(query, limit=self.search_limit):
        #     results.append({
        #         "wikidata_id": hit['id'],
        #         **hit
        #     })

        return results


# v1 - alias 83
# v3 - alias 86
# v3 - alias+gram 89
#             source        method  search_limit  candidates   rank  found
# 0  wikidata-whoosh                       512.0       30.93  15.99   0.86
# 0  wikidata-whoosh    lang-count         512.0       30.93    3.4   0.86
# 0  wikidata-whoosh  lang-count           32.0        21.06    1.5   0.83

if __name__ == '__main__':
    from search_dataset import SEARCH_DATASET, evaluate
    import pandas as pd
    from utils import reset_working_directory

    reset_working_directory()
    index_dir = "data/whoosh-v3/"
    assert SplitTokenizer
    searcher: Searcher = open_dir(index_dir).searcher()

    search_limit = 512
    dataset = SEARCH_DATASET[:]
    data = []
    data += evaluate(
        dataset,
        WhooshSearch(
            searcher=searcher,
            search_limit=search_limit,
            name='wikidata-whoosh',
        ),
        search_limit,
    )

    data = pd.DataFrame.from_records(data)
    print(data[~data.found]['name'].to_list())
    print(data.groupby(['source', "method"]).mean(numeric_only=True).reset_index().round(2))
