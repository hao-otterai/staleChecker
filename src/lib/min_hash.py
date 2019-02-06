import numpy as np
import mmh3

class MinHash(object):
    def __init__(self, k, random_seed=50):
        self._k = k
        self._random_seed = random_seed
        self._masks = (np.random.RandomState(seed=self._random_seed).randint(np.iinfo(np.int64).min, np.iinfo(np.int64).max, self._k))

    def update_min_hash_signature(self, word, min_hash_signature):
        root_hash = mmh3.hash64(word.encode("ascii", "ignore"))[0]
        # root_hash = mmh3.hash64(pickle.dumps(word))[0]  # For MinHashing shingles
        word_hashes = np.bitwise_xor(self._masks, root_hash)  # XOR root hash with k randomly generated integers to simulate k hash functions, can add bitroll if there's time
        min_hash_signature = np.minimum(min_hash_signature, word_hashes)
        return min_hash_signature

    def calc_min_hash_signature(self, tokens):
        min_hash_signature = np.empty(self._k, dtype=np.int64)
        min_hash_signature.fill(np.iinfo(np.int64).max)
        for token in tokens:
            min_hash_signature = self.update_min_hash_signature(token, min_hash_signature)
        return min_hash_signature

    def jaccard_sim_score(self, x, y):
        intersection = set(list(x)).intersection(set(list(y)))
        union = set(list(x)).union(set(list(y)))
        return len(intersection) / (len(union) * 1.0)

    def sim_count(self, x, y):
        return len(set(list(x)).intersection(set(list(y))))

# class MinHash(object):
#     '''
#     This class implements MinHash algorithm.
#         Calculate signature (hash value) for each item in data set.
#         Return the minimum number of hash value
#     '''
#     hash_func = None
#
#     def customized_hash(data, seed):
#         '''
#         This function implements a customized hash function for MinHash.
#             Data = a data/item you want to get hash value.
#             Seed = a seed number to generate different hash function.
#         '''
#         return (3*int(data) + 13*int(seed)) % 100
#
#     def __init__(self, hash_function=None):
#         '''
#         Constructor
#         '''
#         if hash_function is None: hash_function = customized_hash
#
#         MinHash.hash_func = staticmethod(hash_function)
#
#     @staticmethod
#     def get_value(data_list=[], seed=0):
#         _signatures = []
#         _signature = None
#
#         if DEBUG > 1: print('Minhash.get_signature=>data_list=%s, seed=%d'%(data_list, seed))
#         for data in data_list:
#             _signature = MinHash.hash_func(data, seed)
#             #_signature = (3*int(data) + 13*int(seed)) % 100
#             _signatures.append(_signature)
#         if DEBUG > 1: print('Minhash.get_signature=>_signatures=%s'%(_signatures))
#         return min(_signatures)
