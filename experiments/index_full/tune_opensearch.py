from opensearchpy import OpenSearch
from search_opensearch import OpenSearchSearch, client

import optuna
from functools import lru_cache


@lru_cache
def get_searcher():
    client = OpenSearch(
        hosts=["localhost"],
        http_auth=("admin", "7Tr0ngP@ssw0rdwget"),
        scheme="http",
        port=9200,
    )
    return client


from search_dataset import SEARCH_DATASET, evaluate
import pandas as pd


def objective(trial: optuna.Trial):

    searcher = OpenSearchSearch(
        searcher=client,
        search_limit=search_limit,
        name='wikidata-open-search',
        field_boosts={
            "aliases-a": trial.suggest_float("aliases-a", 0, 1, step=0.05),
            "aliases-b": trial.suggest_float("aliases-b", 0, 1, step=0.05),
            "aliases-c": trial.suggest_float("aliases-c", 0, 1, step=0.05),
            "title-a": trial.suggest_float("title-a", 0, 1, step=0.05),
            "title-b": trial.suggest_float("title-b", 0, 1, step=0.05),
            "title-c": trial.suggest_float("title-c", 0, 1, step=0.05),
            "title-kw": trial.suggest_float("title-kw", 0, 1, step=0.05),
            "aliases-kw": trial.suggest_float("aliases-kw", 0, 1, step=0.05),
            "factor-a": trial.suggest_float("factor-a", 0, 10),
            # "factor-mode": trial.suggest_float("factor-mode", 0, 1),
            # "item-mode": trial.suggest_float("item-mode", 0, 1),
            # "factor-modifier": trial.suggest_float("factor-modifier", 0, 1),
        }
    )

    data = pd.DataFrame.from_records(
        evaluate(
            SEARCH_DATASET[::2],
            searcher,
            search_limit,
        )
    )

    rank = data['rank'].mean()
    found = data['found'].mean()
    return round(found, 3), round(rank, 3)


search_limit = 16
study = optuna.create_study(directions=["maximize", "minimize"])  # directions=["maximize", "minimize"])
study.optimize(objective, n_trials=100)

for trial in study.best_trials:
    print(trial.values, trial.params)

# [0.917, 5.248] {'aliases-a': 0.4829718114887267, 'aliases-b': 0.12973846150189694, 'aliases-c': 0.36047571176728177, 'title-a': 0.4700892493030385, 'title-b': 0.10164543211878929, 'title-c': 0.04870754236286101, 'title-kw': 0.00876566393138567, 'aliases-kw': 0.8761695301049854, 'factor-a': 0.07785642708726681}
# [0.924, 6.336] {'aliases-a': 0.17907659116565466, 'aliases-b': 0.9800831882687093, 'aliases-c': 0.1901811335365765, 'title-a': 0.11766649750958846, 'title-b': 0.5354962306571437, 'title-c': 0.17202132009551674, 'title-kw': 0.4653912475965112, 'aliases-kw': 0.7919284147288651, 'factor-a': 0.743677932042815}

# [0.894, 4.314] {'aliases-a': 0.4327018605152626, 'aliases-b': 0.5202206586312705, 'aliases-c': 0.5293207530388081, 'title-a': 0.7291894852227822, 'title-b': 0.016875593764779984, 'title-c': 0.09116420571550454, 'title-kw': 0.4986998054396268, 'aliases-kw': 0.9403927072052848, 'factor-a': 0.6027421566140834}