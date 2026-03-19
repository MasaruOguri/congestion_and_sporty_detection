import os
import pickle
import warnings
warnings.simplefilter("ignore")


# pklファイル保存関数
def save_pkl(directory, filename, data):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, filename), "wb") as f:
        pickle.dump(data, f)


# pklファイル読込関数
def load_pkl(directory, filename):
    with open(os.path.join(directory, filename), "rb") as f:
        return pickle.load(f)
