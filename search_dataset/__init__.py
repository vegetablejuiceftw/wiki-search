import pandas as pd
from tqdm.auto import tqdm

from utils import reset_working_directory

reset_working_directory()

df = pd.read_csv('search_dataset/search-dataset.csv')
df = df[~df['text'].isna() & ~df['id'].isna() & ~df['class'].isna()]
df = df[df['text'].str.len() > 32]
del df['text2']
df = df.reset_index(drop=True)

SEARCH_DATASET = df.to_dict(orient='records')


def location(idx, arr):
    try:
        return arr.index(idx)
    except ValueError:
        return None


def evaluate(dataset, model, limit=30):
    data = []
    for row in tqdm(dataset):
        for annotated_idx in row['id'].split(';'):
            results = model.search(row)
            ids = [r['wikidata_id'] for r in results]
            rank = location(annotated_idx, ids)
            data.append({
                'source': model.name,
                **row,
                'target': annotated_idx,
                'candidates': len(ids),
                'position': rank if rank is not None else limit,
                'rank': rank,
                'found': rank is not None,
                'ids': ids,
            })
    return data
