import pickle
import json



__author__ = "Your Name"

def LoadPickledFile(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

def DumpDataToPickle(data, path):
    with open(path, 'wb') as f:
        pickle.dump(data, f)

def LoadJsonFile(path):
    with open(path, 'r') as f:
        return json.load(f)

def DumpToJsonFile(data,path):
    with open(path, 'w') as f:
        return json.dump(data, f)
