from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import os
import shutil
from typing import List

from tqdm.auto import tqdm

from diskstorage import DiskSearch
from utils import reset_working_directory

reset_working_directory()

# Initialize OpenSearch client
client = OpenSearch(
    hosts=["localhost"],
    http_auth=("admin", "7Tr0ngP@ssw0rdwget"),  # Change to your admin credentials
    scheme="http",
    port=9200,
)

index_name = "estonia_index"
index_name = "search_index_v2"

try:
    client.indices.delete(index=index_name)
except Exception as e:
    print("fail", e)

# index_settings = {
#     "properties": {
#         "aliases": {
#             "type": "text",
#             "fields": {
#                 "keyword": {
#                     "type": "keyword"
#                 },
#                 "lowercase": {  # Add a sub-field for case-insensitive searching
#                     "type": "text",
#                     "analyzer": "lowercase_analyzer"
#                 }
#             }
#         }
#     },
#     "settings": {
#         "analysis": {
#             "analyzer": {
#                 "lowercase_analyzer": {
#                     "tokenizer": "keyword",  # Use keyword tokenizer to keep original token
#                     "filter": "lowercase"    # Apply lowercase filter to tokens
#                 }
#             }
#         }
#     }
# }
# client.indices.create(index=index_name, body=index_settings)
mapping_body = {
    "properties": {
        "aliases": {
            "type": "keyword",
            #  "filter": "lowercase",
            # "analyzer": "lowercase_analyzer"
        }
    }
}

# Define the settings for the custom analyzer
# settings_body = {
#     "analysis": {
#         "analyzer": {
#             "lowercase_analyzer": {
#                 "tokenizer": "keyword",
#                 "filter": "lowercase"
#             }
#         }
#     }
# }

client.indices.create(index=index_name)#, body={"settings": settings_body})
client.indices.put_mapping(index=index_name, body=mapping_body)


# docker kill $(docker ps -q)

dataset = DiskSearch(f'data/wikidata-v3.cache')
print(dataset['Q191'])
total_rows = 27_420_075 or len(tuple(dataset.keys()))
rows = (
    row
    for key, row in tqdm(dataset.iter(), total=total_rows)
    if row['text'] and key.startswith('Q')# and row.get("count_languages", 0) > 0 # and any(k in a.lower() for a in row['aliases'] for k in ['est',]) and row.get("count_languages", 0) > 5
)

# # Define a generator function to yield documents in bulk
def document_generator(rows):
    for row in rows:
        title = (row['labels'] + row['aliases'] + [row['text']])[0]
        aliases = [a.lower() for a in row.get("aliases", [])]
        yield {
            "_index": index_name,
            "_id": row["id"],  # Assuming "id" is unique and can be used as the document ID
            "_source": {
                "title": title,
                "text": row["text"],
                "aliases": aliases,
                "popularity": row.get("count_languages", 0)
            }
        }

# Index the documents in bulk
bulk(client, document_generator(rows))
print("Documents added successfully.")


# Define your documents to be indexed
# rows = [
#     {
#         "id": "Q191",
#         "title": "Estonia",
#         "text": "Estonia is known for its cultural heritage",
#         "aliases": ["Eesti"],
#         "popularity": 1
#     },
#     # Add more documents as needed
# ]