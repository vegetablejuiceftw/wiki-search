from functools import lru_cache

import spacy
from tqdm.auto import tqdm
from difflib import SequenceMatcher

from search_dataset import SEARCH_DATASET

nlp = spacy.load("en_core_web_trf")


@lru_cache
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


@lru_cache
def get_phrases(name, text):
    doc = nlp(text)
    noun_phrases = set(c.text for c in doc.noun_chunks) - {name}
    noun_phrases = list(
        t for t in noun_phrases
        if (len(t) > 4 and similar(t.lower(), name.lower()) > 0.5 or name.lower() in t.lower()))
    noun_phrases.insert(0, name)
    return noun_phrases


if __name__ == '__main__':

    dataset = SEARCH_DATASET
    for value in (dataset):
        name = value['name']
        text = value['text']
        noun_phrases = get_phrases(name, text)
        print(name, noun_phrases)
