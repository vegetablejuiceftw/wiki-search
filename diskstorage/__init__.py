import dbm.gnu as dbm
import msgpack


class DiskSearch:

    @staticmethod
    def write(file_path: str, items: list):
        with dbm.open(file_path, 'c') as db:
            for item in items:
                db[b'hello'] = msgpack.packb(item)

    @staticmethod
    def read(file_path: str, key: str):
        with dbm.open(file_path, 'c') as db:
            item = db.get(key, None)
            if item is not None:
                return msgpack.unpackb(item)
