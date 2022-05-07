import os
import numpy as np
from tqdm import tqdm
from glob import glob


malware_paths = glob("data/dataset/malware/*.adjlist")
benign_paths = glob("data/dataset/benign/*.adjlist")

unused_malware = np.random.choice(malware_paths, size=len(malware_paths)-400)
unused_benign = np.random.choice(benign_paths, size=len(benign_paths)-600)
unused_paths = list(unused_malware) + list(unused_benign)

for path in tqdm(unused_paths):
    try:
        os.remove(path)
    except:
        pass