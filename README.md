# wiki-search
Wikipedia / Wikidata search project for knowledge base RAG systems.

```
             source              method  search_limit  candidates  rank  found
0  search_wikipedia          WikiSearch          16.0       16.00  1.97   0.75
1  search_wikipedia  WikiSearchExpanded          16.0       24.61  2.65   0.79
2   search_wikidata          WikiSearch          16.0       14.23  1.51   0.85
3   search_wikidata  WikiSearchExpanded          16.0       17.74  1.82   0.86
4       search_both          WikiSearch          16.0       27.53  2.56   0.92
5       search_both  WikiSearchExpanded          16.0       39.21  3.02   0.93

              source       method  candidates  rank  found
0  wikidata-5m-alias  AliasSearch        1.45  0.28   0.35
1      wikidata-full  AliasSearch       26.90  6.56   0.84
2     wikidata-fuzzy  FuzzySearch       66.36  6.33   0.90
2     wikidata-fuzzy  w-phrases         88.90  8.76   0.93

                    source           method  search_limit  candidates  distance  rank  found
0  wikidata-full-emb-faiss  EmbeddingSearch           8.0      102.99      0.76  8.63   0.89
```


## Notes:

Qdrant - does not support text search
marqo - does not support fuzzy search
