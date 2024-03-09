import os
import shutil
from utils import reset_working_directory

reset_working_directory()

from tqdm.auto import tqdm
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser

from diskstorage import DiskSearch
from search_index.helpers import schema

VERSION = "v3.micro"
index_dir = f"data/whoosh-{VERSION}/"

CREATE = True
if CREATE:
    if os.path.exists(index_dir):
        print("DEL")
        shutil.rmtree(index_dir)

    if not os.path.exists(index_dir):
        print("CREATE")
        os.mkdir(index_dir)

    dataset = DiskSearch(f'data/wikidata-v3.cache')
    print(dataset['Q191'])

    ix = create_in(index_dir, schema)

    # Get a writer for the index
    writer = ix.writer(limitmb=1024 * 2, procs=12, multisegment=True)

    total_rows = 34_000_000 or len(tuple(dataset.keys()))
    rows = (
        row
        for key, row in tqdm(dataset.iter(), total=total_rows)
        if row['text'] and key.startswith('Q')  # or row['aliases']
    )

    for row in rows:
        aliases = ",,".join(a for a in row['aliases'] if a)
        labels = ",,".join(a for a in row['labels'] if a)
        title = (row['labels'] + row['aliases'] + [row['text']])[0]
        schema.items()
        writer.add_document(
            id=row['id'],
            # label=labels,
            alias=aliases,
            alias_ngram=aliases,
            # title=title,
            # description=row['text'],
            # alias_count=row['count_aliases'],
            language_count=row['count_languages'],
        )

    print("Commit changes to the index")
    writer.commit()

    print("Optimize index")
    writer = ix.writer(limitmb=1024 * 2, procs=12, multisegment=True)
    writer.commit(optimize=True)

ix = open_dir(index_dir)
searcher = ix.searcher()

for query_string in [
    "estonia",
    "electric lighting",
]:
    query_parser = QueryParser("alias_ngram", schema=ix.schema)
    query = query_parser.parse(f"'{query_string}'")
    print(query_string, query)

    results = searcher.search(query, limit=20, scored=True)
    print(len(results))

    for hit in results:
        print(hit)
    print("DONE")
