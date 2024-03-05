##

from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from whoosh.searching import Searcher

from diskstorage import DiskSearch
from search_index.helpers import SplitTokenizer


from utils import reset_working_directory

reset_working_directory()
index_dir = "data/whoosh/"
assert SplitTokenizer
searcher: Searcher = open_dir(index_dir).searcher()

cache = DiskSearch('data/wikidata-v2.aliases-lc.cache')

res_fuzzy = ['fertilizing', 'fertilizing', 'Anthropic', 'Lint', 'two worlds two', 'baked', 'Wan show', 'Swift', 'diablo four', 'fan', 'wd forty', 'arm', 'arm', 'Prime', 'Baruto', 'Boruto', 'Cerberus', 'pandas', 'seizure']

res_whoosh = ['king', 'king', 'Transformers', 'Cheating', 'alien', 'alien', 'baked', 'baking soda', 'Wan show', 'Ajax', 'fabric', 'Robinhood', 'op', 'op', 'squash ', 'The game', 'vs code', 'fan', 'wd forty', 'arms', 'arm', 'arm', 'Prime', 'Baruto', 'Monster', 'Kiwi ', 'Kiwi', 'Cerberus', 'Indians', 'pandas', 'absolute territory', 'seizure', 'rag', 'rag doll']

broken = set(res_whoosh) - set(res_fuzzy)

print(broken)


##
cases = {"absolute territory": "Q196877"}

for q, qid in cases.items():
    # r = cache[query]
    # print(query, r, qid, qid in (r or []))

    query_parser = QueryParser("alias", schema=searcher.schema)
    query = query_parser.parse(repr(q))

    results = []
    for hit in searcher.search(query, limit=128):
        results.append(hit['id'])

    print(query, results, qid, qid in results)

    query_parser = QueryParser("title", schema=searcher.schema)
    query = query_parser.parse(q)

    results = []
    for hit in searcher.search(query, limit=128):
        results.append(hit['id'])

    print(query, results, qid, qid in results)

    results = []
    for hit in searcher.documents(alias=q):
        results.append(hit['id'])

    print(q, results, qid, qid in results)
