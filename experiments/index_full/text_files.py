from diskstorage import DiskSearch
from utils import reset_working_directory

reset_working_directory()

embedding_cache = 'data/embeddings/v6/wikidata.emb.cache'
index_cache = 'data/embeddings/v6/wikidata.index.cache'
# 49871759

dataset = DiskSearch(index_cache)
print(len(tuple(dataset.keys())))
