from random import sample

import numpy as np

from diskstorage import DiskSearch
from utils import reset_working_directory

reset_working_directory()

total_rows = 44888496

embedding_cache = 'data/embeddings/wikidata.emb.v4.npy'
embeddings_fp = np.memmap(embedding_cache, dtype='float32', mode='r', shape=(total_rows, 384))

print(embeddings_fp.shape)

import faiss
from tqdm.auto import tqdm

nlist = 10000

index_path = 'data/emb_index.v5.faiss'
#
# from autofaiss import build_index
#
# build_index(embeddings=embeddings_fp, index_path=index_path,
#             index_infos_path=f"{index_path}.json", max_index_memory_usage="56G",
#             current_memory_available="64G", max_index_query_time_ms=33.0, min_nearest_neighbors_to_retrieve=32)

# coarse_quantizer = faiss.IndexHNSWFlat(384, 32)
# index = faiss.IndexIVFPQ(coarse_quantizer, 384, nlist, 16, 8)
#
# # quantizer = faiss.IndexFlatIP(384)
# # index = faiss.IndexIVFPQ(quantizer, 384, nlist, 16, 8)
# batch_size = 4_000_000
#
#
# index.train(embeddings_fp[::5])
#
# # Add the vectors to the index in batches
# for i in tqdm(range(0, total_rows, batch_size)):
#     batch = embeddings_fp[i:i + batch_size]
#     index.add(batch)
# faiss.write_index(index, index_path)

index = faiss.read_index(index_path)

key_index = DiskSearch("data/key_index.cache")

total = 256

ids = [int(i) for qid in range(1, total+1) for i in key_index[f"Q{qid}"] or []]
ids = ids[:total]
found = []
distances = []
for idx in tqdm(ids):
    emb = embeddings_fp[idx:idx+1,:]

    D, I = index.search(emb, 16)

    f = False
    distance = None
    for i in range(1):
        for j in range(16):
            neighbor_index = I[i, j]
            if neighbor_index == idx:
                distance = D[i, j].item()
                f = True
                break

    found.append(f)
    if distance is not None:
        distances.append(distance)


print(sum(found) / total)
print(sum(distances)/len(distances))

#
# nlist = 4096  # Number of clusters
# index = faiss.IndexIVFFlat(faiss.IndexFlatIP(384), 384, nlist)
# print("A")
# index.train(embeddings_fp)
# print("C")
# index.add(embeddings_fp)
# print("D")
#
# # Save the index to disk
# faiss.write_index(index, index_path)


# import faiss
# import numpy as np
#
# # Create the read-only index
# index = faiss.IndexIDMap(faiss.IndexFlatIP(384))
#
# # Load the vectors in batches and add them to the index
# batch_size = 1_000_000
# for start in tqdm(range(0, total_rows, batch_size)):
#     end = min(start + batch_size, total_rows)
#     index.add_with_ids(embeddings_fp[start:end], np.arange(start, end))
#
# # Save the index to disk
# faiss.write_index(index, index_path)
#
# import lance
# from tqdm import tqdm
# from lance.vector import vec_to_table
# # Example usage
# batch_size = 1_000_000
# index_path = 'data/index.lance'
#
# vt = vec_to_table(embeddings_fp)
#
# sift1m = lance.write_dataset(vt, index_path, max_rows_per_group=8192, max_rows_per_file=1024*1024)


# import hnswlib
#
# def build_index_in_parts(embeddings_fp, index_path, total_rows, dim=384, ef_construction=200, M=64):
#     batch_size = 4_000_000
#
#     parts = []
#
#     for start in tqdm(range(0, total_rows, batch_size)):
#         end = start + batch_size
#
#         p = hnswlib.Index(space='ip', dim=dim)
#         p.init_index(max_elements=end - start, ef_construction=ef_construction, M=M)
#
#         batch = embeddings_fp[start:end]
#         p.add_items(batch)
#
#         part_index_path = f"{index_path}.part{len(parts)}"
#         p.save_index(part_index_path)
#         parts.append(part_index_path)
#
#     # Merge the parts
#     p = hnswlib.Index(space='ip', dim=dim)
#     p.load_index(f"{index_path}.part0", max_elements=total_rows)
#     for part_index_path in parts:
#         p.add_index(part_index_path)
#
#     p.save_index(index_path)
#
#
# index = build_index_in_parts(embeddings_fp, 'data/index.hnswlib', total_rows)


# import faiss
# import numpy as np
# from tqdm.auto import tqdm
#
# def build_index(embeddings_fp, output_file, batch_size=1_000_000):
#     total_rows = embeddings_fp.shape[0]
#     dim = embeddings_fp.shape[1]
#
#     # Create an empty index
#     index = faiss.IndexIDMap(faiss.IndexFlatIP(dim))
#
#     # Add vectors in batches
#     for i in tqdm(range(0, total_rows, batch_size)):
#         batch = embeddings_fp[i:i:i + batch_size]
#         index.add_with_ids(batch, np.arange(i, i + batch.shape[0]))
#
#     # Save the index to disk
#     faiss.write_index(index, output_file)
#
# def merge_indexes(input_files, output_file):
#     indexes = [faiss.read_index(file) for file in input_files]
#     merged_index = faiss.IndexIDMap(faiss.IndexFlatIP(indexes[0].d))
#     for index in tqdm(indexes):
#         merged_index.add_with_ids(index.xb, index.ids)
#     faiss.write_index(merged_index, output_file)

# batch_size = 1_000_000
# num_batches = total_rows // batch_size + 1
# for i in range(num_batches):
#     start = i * batch_size
#     end = min((i + 1) * batch_size, total_rows)
#     build_index(embeddings_fp[start:end], f'data/index_{i}.faiss', batch_size=batch_size)
#
# input_files = [f'data/index_{i}.faiss' for i in range(num_batches)]
# merge_indexes(input_files, 'data/merged_index.faiss')