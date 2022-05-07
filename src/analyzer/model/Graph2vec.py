"""Graph2Vec module."""

import os
import glob
import hashlib
import pandas as pd
import networkx as nx
from tqdm import tqdm
from joblib import Parallel, delayed
from gensim.models.doc2vec import Doc2Vec, TaggedDocument


INPUT_PATH = "./data/dataset/"
# OUTPUT_PATH = "./data/embeddings.csv"
OUTPUT_PATH = "./data/embeddings_ae.csv"
DIMENSIONS = 128
WORKERS = os.cpu_count()
EPOCHS = 5
MIN_COUNT = 5
WL_ITERATIONS = 2
LEARNING_RATE = 0.025
DOWN_SAMPLING = 0.0001


class WeisfeilerLehmanMachine:
    """
    Weisfeiler Lehman feature extractor class.
    """

    def __init__(self, graph, features, iterations):
        """
        Initialization method which also executes feature extraction.
        :param graph: The Nx graph object.
        :param features: Feature hash table.
        :param iterations: Number of WL iterations.
        """
        self.iterations = iterations
        self.graph = graph
        self.features = features
        self.nodes = self.graph.nodes()
        self.extracted_features = [str(v) for k, v in features.items()]
        self.do_recursions()

    def do_a_recursion(self):
        """
        The method does a single WL recursion.
        :return new_features: The hash table with extracted WL features.
        """
        new_features = {}
        for node in self.nodes:
            nebs = self.graph.neighbors(node)
            degs = [self.features[neb] for neb in nebs]
            features = [str(self.features[node])] + \
                sorted([str(deg) for deg in degs])
            features = "_".join(features)
            hash_object = hashlib.md5(features.encode())
            hashing = hash_object.hexdigest()
            new_features[node] = hashing
        self.extracted_features = self.extracted_features + \
            list(new_features.values())
        return new_features

    def do_recursions(self):
        """
        The method does a series of WL recursions.
        """
        for _ in range(self.iterations):
            self.features = self.do_a_recursion()


def path2name(path):
    base = os.path.basename(path)
    return os.path.splitext(base)[0]


def dataset_reader(path):
    """
    Function to read the graph and features from a json file.
    :param path: The path to the graph json.
    :return graph: The graph object.
    :return features: Features hash table.
    :return name: Name of the graph.
    """
    name = path2name(path)
    graph = nx.read_adjlist(path, create_using=nx.DiGraph)

    graph = nx.relabel_nodes(
        graph, mapping=dict(zip(graph, range(len(graph)))))

    features = nx.degree(graph)
    features = {int(k): v for k, v in features}

    return graph, features, name


def feature_extractor(path, rounds):
    """
    Function to extract WL features from a graph.
    :param path: The path to the graph json.
    :param rounds: Number of WL iterations.
    :return doc: Document collection object.
    """
    graph, features, name = dataset_reader(path)
    machine = WeisfeilerLehmanMachine(graph, features, rounds)
    doc = TaggedDocument(words=machine.extracted_features, tags=["g_" + name])
    return doc


def save_embedding(output_path, model, files, dimensions):
    """
    Function to save the embedding.
    :param output_path: Path to the embedding csv.
    :param model: The embedding model object.
    :param files: The list of files.
    :param dimensions: The embedding dimension parameter.
    """
    out = []
    for f in files:
        identifier = path2name(f)
        out.append([identifier] + list(model.dv["g_"+identifier]))
    column_names = ["type"]+["x_"+str(dim) for dim in range(dimensions)]
    out = pd.DataFrame(out, columns=column_names)
    out = out.sort_values(["type"])
    out.to_csv(output_path, index=None)
    return out


def Graph2vec():
    """
    Main function to read the graph list, extract features.
    Learn the embedding and save it.
    :param args: Object with the arguments.
    """
    graphs = glob.glob(os.path.join(INPUT_PATH, "*/*.adjlist"))

    document_collections = Parallel(n_jobs=WORKERS)(
        delayed(feature_extractor)(g, WL_ITERATIONS) for g in tqdm(graphs))

    model = Doc2Vec(document_collections,
                    vector_size=DIMENSIONS,
                    window=0,
                    min_count=MIN_COUNT,
                    dm=0,
                    sample=DOWN_SAMPLING,
                    workers=WORKERS,
                    epochs=EPOCHS,
                    alpha=LEARNING_RATE)

    out = save_embedding(OUTPUT_PATH, model, graphs, DIMENSIONS)
    return out


if __name__ == "__main__":
    Graph2vec()
