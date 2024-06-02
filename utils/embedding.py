import os
from functools import lru_cache
from time import sleep

from multiprocessing import Lock

gpu_select_lock = Lock()

@lru_cache
def load_st1(device=None):
    import torch
    from sentence_transformers import SentenceTransformer
    if device:
        print("DEVICE GIVEN", device)
        model = SentenceTransformer('all-MiniLM-L6-v2', device=torch.device(device))
        print(model.device)
    else:
        with gpu_select_lock:
            index, mem = get_gpu_with_most_free_memory()
            print(index, mem)
            device = torch.device('cpu') if index is None or mem > 100 else torch.device(f'cuda:{index}')
            print(f"starting on {device}/{mem}")
            model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
            print(model.device)

    encoder = model.encode
    return encoder, model, 384


@lru_cache
def load_use4():
    with gpu_select_lock:
        index, _ = get_gpu_with_most_free_memory()
        if index is None:
            device = 'CPU'
        else:
            device = f'GPU:{index}'

        print(f"starting on {device}")
        if index is not None:
            os.environ["CUDA_VISIBLE_DEVICES"] = str(index)
        else:
            os.environ["CUDA_VISIBLE_DEVICES"] = "NONE"

        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

        import tensorflow as tf

        import tensorflow_hub as hub
        with tf.device(device):
            encoder = embed = hub.load("https://tfhub.dev/google/universal-sentence-encoder/4")
            print(device, embed(['foo']).device)
        sleep(0.1)
    sleep(0.1)
    return encoder, embed, 512


def get_gpu_with_most_free_memory(threshold=9000):  # Threshold in MB, default 4GB
    import GPUtil

    gpus = GPUtil.getGPUs()
    gpu_with_most_memory = None
    max_memory = threshold
    
    for gpu in gpus:
        # print("FREE", gpu.id, gpu.memoryFree)
        if gpu.memoryFree > max_memory:
            gpu_with_most_memory = gpu
            max_memory = gpu.memoryFree

    return (
        gpu_with_most_memory.id if gpu_with_most_memory else None,
        gpu_with_most_memory.memoryUsed if gpu_with_most_memory else None,
    )
