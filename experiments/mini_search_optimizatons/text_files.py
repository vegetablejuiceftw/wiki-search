from diskstorage import DiskSearch
from utils import reset_working_directory

reset_working_directory()

embedding_cache = 'data/embeddings/v5/wikidata.emb.cache'
index_cache = 'data/embeddings/v5/wikidata.index.cache'


dataset_index = DiskSearch(index_cache)
print(len(tuple(dataset_index.keys())))
