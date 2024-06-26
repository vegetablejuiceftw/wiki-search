from itertools import chain
from typing import Callable

import requests
from pydantic import BaseModel

from noun_phases import get_phrases
from utils import disk_cached


@disk_cached()
def search_wikipedia(keyword, search_limit=50):
    base_url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srsearch": keyword,
        'srlimit': search_limit,
    }

    results = []
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
        result = response.json()
        search_results = result.get("query", {}).get("search", [])

        if search_results:
            page_ids = [result["pageid"] for result in search_results]
            wikidata_ids = get_wikidata_ids(*page_ids)
            for result, wikidata_id in zip(search_results, wikidata_ids):
                result['wikidata_id'] = wikidata_id
                results.append(result)

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

    return results


@disk_cached()
def get_wikidata_ids(*page_ids):
    base_url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "format": "json",
        "pageids": "|".join(map(str, page_ids)),  # Convert list to pipe-separated string
        "prop": "pageprops",
        "inprop": "url"
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
        result = response.json()
        pages = result.get("query", {}).get("pages", {})
        wikidata_ids = []
        for page_id in page_ids:
            # Extract Wikidata ID from the API response
            wikidata_id = pages.get(str(page_id), {}).get("pageprops", {}).get("wikibase_item", "")
            wikidata_ids.append(wikidata_id)

        return wikidata_ids

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

    return [None] * len(page_ids)


@disk_cached()
def search_wikidata(keyword, search_limit=5):
    base_url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "search": keyword,
        "language": "en",
        "limit": search_limit,
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes
        result = response.json()

        # Extract relevant information from the API response
        search_results = result.get("search", [])
        wikidata_ids = []

        for result in search_results:
            label = result.get("label", "")
            entity_id = result.get("id", "")
            result['wikidata_id'] = entity_id
            # print(f"Label: {label}, Wikidata ID: {entity_id}")
            wikidata_ids.append(result)

        return wikidata_ids

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
    return []


def search_both(keyword, search_limit=50):
    results = []
    results += search_wikidata(keyword, search_limit=search_limit)
    results += search_wikipedia(keyword, search_limit=search_limit)
    visited = set()
    out = []
    for r in results:
        if r['wikidata_id'] in visited:
            continue
        visited.add(r['wikidata_id'])
        out.append(r)
    return out


class WikiSearch(BaseModel):
    search_limit: int
    func: Callable

    def search(self, row):
        mention_name, annotated_class, annotated_idx = row['name'], row['class'], row['id']
        return self.func(mention_name, search_limit=self.search_limit)

    @property
    def source(self):
        return self.func.__name__

    @property
    def method(self):
        return self.__class__.__name__


class WikiSearchExpanded(WikiSearch):
    def search(self, row):
        mention_name, annotated_text, annotated_idx = row['name'], row['text'], row['id']
        phrases = get_phrases(mention_name, annotated_text)
        return list({
            result['wikidata_id']: result
            for result in chain(*[
                self.func(p, search_limit=self.search_limit)
                for p in phrases
            ])
        }.values())


if __name__ == '__main__':
    from search_dataset import SEARCH_DATASET, evaluate
    import pandas as pd

    search_limit = 16
    dataset = SEARCH_DATASET[:]
    data = []
    data += evaluate(dataset, WikiSearch(search_limit=search_limit, func=search_wikipedia), search_limit)
    data += evaluate(dataset, WikiSearch(search_limit=search_limit, func=search_wikidata), search_limit)
    data += evaluate(dataset, WikiSearch(search_limit=search_limit, func=search_both), search_limit)

    data += evaluate(dataset, WikiSearchExpanded(search_limit=search_limit, func=search_wikipedia), search_limit)
    data += evaluate(dataset, WikiSearchExpanded(search_limit=search_limit, func=search_wikidata), search_limit)
    data += evaluate(dataset, WikiSearchExpanded(search_limit=search_limit, func=search_both), search_limit)

    data = pd.DataFrame.from_records(data)
    print(data.groupby(['source', 'method']).mean(numeric_only=True).sort_values("found").reset_index().round(2))
