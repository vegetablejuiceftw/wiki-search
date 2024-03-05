from rapidfuzz import process, fuzz
from diskstorage import DiskSearch
from noun_phases import get_phrases
from itertools import chain
from functools import lru_cache

from search_dataset.base import BaseSearch


class FuzzySearch(BaseSearch):
    keys: tuple
    cache: DiskSearch
    phrases: bool = False

    def search(self, row):
        mention_name, annotated_text, annotated_idx = (
            row["name"],
            row["text"],
            row["id"],
        )
        if self.phrases:
            phrases = get_phrases(mention_name, annotated_text)
            return self.search_phrases(phrases)

        return self.search_phrases([mention_name])

    @lru_cache
    def fuzzy_search(self, query):
        results = process.extract(query, self.keys, scorer=fuzz.QRatio, limit=self.search_limit, score_cutoff=85)
        out = {}
        for k, m, _ in results:
            for qid in self.cache[k]:
                if qid not in out:
                    out[qid] = [m, k]
        return out

    def search_phrases(self, phrases):
        return list(
            {
                qid: {"wikidata_id": qid}
                for qid in chain(*[self.fuzzy_search(p.lower()) for p in phrases])
            }.values()
        )

    def __hash__(self):
        return hash(self.name)
