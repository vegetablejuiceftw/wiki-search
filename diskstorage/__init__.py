import dbm.gnu as dbm
from typing import Dict, Any, List, Tuple, Iterable

import msgpack


class DiskSearch(dict):

    def __init__(self, file_path):
        self.db = dbm.open(file_path, 'c')

    def __getitem__(self, key):
        return self.read(key)

    def __setitem__(self, key, value):
        self.write([(key, value)])

    def write(self, items: Iterable[Tuple[str, Any]]):
        for key, item in items:
            self.db[key] = msgpack.packb(item)

    def read(self, key: str):
        item = self.db.get(key, None)
        if item is not None:
            return msgpack.unpackb(item)

    def iter(self):
        for key in self.db.keys():
            yield key.decode('utf-8'), msgpack.unpackb(self.db[key])

    def keys(self):
        return (key.decode('utf-8') for key in self.db.keys())
