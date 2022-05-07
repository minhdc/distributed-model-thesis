import networkx as nx
import pandas as pd
import time
from copy import deepcopy
from joblib import load
from Graph2vec import Graph2vec


class Environment():
    def __init__(self, graph):
        self.init_graph = graph
        self.graph = deepcopy(self.init_graph)
        self.init_state = dict()

        self.scaler = load("model/scaler.joblib")
        self.clf = load("model/RF.joblib")

    def reset(self):
        self.graph = deepcopy(self.init_graph)
        return self.init_state

    def step(self, action, state, step):
        reward = -1

        root = action
        for _ in range(5):
            self.graph.add_edge(root, dummy := f"dummy_{time.time()}")
            root = dummy

        nx.write_adjlist(self.graph, "data/dataset/AE/AE.adjlist")
        Graph2vec()

        data = pd.read_csv("data/embeddings.csv")
        x = data.loc[data["type"] == "AE", "x_0":].values
        x = self.scaler.transform(x)
        y = self.clf.predict(x)[0]

        if step == 49:
            reward = -1000
        if y == 0:
            reward = 1000
            done = True
        else:
            done = False

        next_state = dict(state)
        next_state[action] = next_state.get(action, 0) + 1
        next_state = dict(sorted(next_state.items()))
        return next_state, reward, done
