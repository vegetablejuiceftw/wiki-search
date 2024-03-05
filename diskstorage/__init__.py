import dbm.gnu as dbm
from typing import Dict, Any, List, Tuple, Iterable
from noun_phases import get_phrases
from itertools import chain

import msgpack

from search_dataset.base import BaseSearch


class DiskSearch(dict):

    def __init__(self, file_path):
        self.db = dbm.open(file_path, "c")

    def __getitem__(self, key):
        return self.read(key)

    def __setitem__(self, key, value):
        self.write([(key, value)])

    def write(self, items: Iterable[Tuple[str, Any]]):
        for key, item in items:
            self.db[key] = msgpack.packb(item)  # type: ignore

    def read(self, key: str):
        item = self.db.get(key, None)
        if item is not None:
            return msgpack.unpackb(item)

    def iter(self):
        for key in self.db.keys():
            yield key.decode("utf-8"), msgpack.unpackb(self.db[key])

    def keys(self) -> Iterable:  # type: ignore
        return (key.decode("utf-8") for key in self.db.keys())


class AliasSearch(BaseSearch):

    cache: DiskSearch

    def search(self, row):
        mention_name, annotated_text, annotated_idx = (
            row["name"],
            row["text"],
            row["id"],
        )
        phrases = get_phrases(mention_name, annotated_text)
        return self.search_phrases(phrases)
        
    def search_phrases(self, phrases):
        return list(
            {
                qid: {"wikidata_id": qid}
                for qid in chain(*[self.cache.read(p.lower()) or [] for p in phrases])
            }.values()
        )
