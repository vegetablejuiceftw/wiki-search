import os
import time
import unicodedata

import git
import diskcache

ROOT_DIR = git.Repo('.', search_parent_directories=True).working_tree_dir


def normalize(text: str):
    return unicodedata.normalize("NFKD", text.strip()).encode("ascii", "ignore").decode().replace("  ", " ").strip()


def chunking(iterator, size=32):
    iterator = iter(iterator)
    while True:
        out = []
        for e in iterator:
            out.append(e)
            if len(out) >= size:
                break
        if out:
            yield out
        else:
            break


def reset_working_directory():
    os.chdir(ROOT_DIR)


def throttle(pause=1):
    def decorator(func):
        last_call = 0

        def wrapper(*args, **kwargs):
            nonlocal last_call
            current_time = time.time()
            if current_time - last_call < pause:
                time.sleep(pause - (current_time - last_call))
            last_call = time.time()
            return func(*args, **kwargs)

        wrapper.__name__ = func.__name__
        return wrapper

    return decorator


def disk_cached(version=None):
    def decorator(func):
        path = os.path.join(ROOT_DIR, f"data/caches/{func.__name__}-{version}")
        cache = diskcache.Cache(path)

        def wrapper(*args, **kwargs):
            key = (args, frozenset(kwargs.items()))
            key_with_version = (version, key)
            if key_with_version in cache:
                return cache[key_with_version]

            result = func(*args, **kwargs)
            cache[key_with_version] = result
            return result

        wrapper.__name__ = func.__name__
        return wrapper

    return decorator
