from whoosh.fields import Schema, TEXT, ID, KEYWORD, NUMERIC, NGRAMWORDS
from whoosh.analysis import LowercaseFilter, Tokenizer, Token, StripFilter, SubstitutionFilter
from whoosh.util.text import rcompile


class SplitTokenizer(Tokenizer):
    def __init__(self, expression=r",,"):
        self.expr = rcompile(expression)

    def __call__(
            self,
            value,
            positions=False,
            chars=False,
            keeporiginal=False,
            removestops=True,
            start_pos=0,
            start_char=0,
            tokenize=True,
            mode="",
            **kwargs
    ):
        if value is None:
            return

        # Split the input text using ",," as separator
        tokens = self.expr.split(value)
        token = Token(positions, chars, removestops=removestops, mode=mode, **kwargs)

        for t in tokens:
            token.text = t
            yield token


split_analyzer = SplitTokenizer() | StripFilter() | LowercaseFilter() | SubstitutionFilter(r"\w\.", "")

schema = Schema(
    id=ID(unique=True, stored=True),
    # label=KEYWORD(stored=True, scorable=True, analyzer=split_analyzer, field_boost=2.0),
    alias=KEYWORD(analyzer=split_analyzer),
    alias_ngram=NGRAMWORDS(tokenizer=split_analyzer, minsize=2, maxsize=2),
    # title=TEXT(lang="en", field_boost=2.0),
    # description=NGRAMWORDS(minsize=2, maxsize=2),
    # alias_count=NUMERIC(sortable=True),
    language_count=NUMERIC(sortable=True),
)
# for field in ['title', 'description']:
#     if field in schema:
#         print(field)
#         stem_ana = schema[field].format.analyzer
#         # Set the cachesize to -1 to indicate unbounded caching
#         stem_ana.cachesize = -1
