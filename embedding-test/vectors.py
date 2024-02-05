import torch

print(torch.cuda.is_available())  # should be True

t = torch.rand(10, 10).cuda()
print(t.device)  # should be CUDA

from sentence_transformers import SentenceTransformer, util
model = SentenceTransformer('all-MiniLM-L6-v2', device=torch.device('cuda:0'))

# model.to('cuda')
print(model.device)

import tensorflow_hub as hub

module_url = "https://tfhub.dev/google/universal-sentence-encoder/4"
embed = hub.load(module_url)
print(embed(['foo bar']))
