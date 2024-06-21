from fastembed import TextEmbedding
from typing import List

# Example list of documents
documents: List[str] = [
    "This is built to be faster and lighter than other embedding libraries e.g. Transformers, Sentence-Transformers, etc.",
    "fastembed is supported by and maintained by Qdrant.",
]

# This will trigger the model download and initialization
embedding_model = TextEmbedding(model_name="sentence-transformers/all-MiniLM-L6-v2")

embeddings_generator = embedding_model.embed(documents)  # reminder this is a generator
embeddings_list = list(embedding_model.embed(documents))
# you can also convert the generator to a list, and that to a numpy array
print(len(embeddings_list[0]))
