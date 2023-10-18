# retrieval_models

main.py: to train model.
inference.py: to extract vectors from model and do u2i and i2i extenstionn.


def findNeighbors2(partition_data):
    b_index = broadcasted_index.value
    b_vectors = broadcasted_vectors.value

    _iterator = []

    multiple_vectors = None

    item_ids = []

    for row in partition_data:

        item_ids.append(row.item_id)
        vector = np.array(row.embedding).reshape(1, -1).astype("float32")

        multiple_vectors = (
            np.append(multiple_vectors, vector, axis=0)
            if multiple_vectors is not None
            else vector
        )

    # use batch search instead of single search
    _, I = b_index.search(multiple_vectors, topK + 1)

    for _id, each in enumerate(I):
        _iterator.append([item_ids[_id], b_vectors.loc[each]["item_id"].tolist()[1:]])

    return iter(_iterator)

index = faiss.IndexFlatL2(emb_size)   # build the index
print(index.is_trained)
index.add(np.array(vectors['embedding'].tolist()).astype("float32"))                  # add vectors to the index
vectors.columns = ['item_id','embedding']
vectors = vectors[['item_id']]

broadcasted_vectors = sc.broadcast(vectors)

broadcasted_index = sc.broadcast(index)

i2i = item_emb.rdd.repartition(int(item_emb.count()/5000))\
            .mapPartitions(findNeighbors2)\
            .toDF(['item_id','sim_item_id'])
