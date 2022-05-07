import os
import numpy as np
import networkx as nx
import shutil
from glob import glob
from copy import deepcopy
from Environment import Environment
from QLearningTable import QLearningTable


def update(num_episode, env, RL):
    avail_action = RL.get_all_actions()

    for e in range(num_episode):
        print(f"Episode {e}/{num_episode}")
        step = 0
        state = env.reset()

        while step < 10:
            action = RL.choose_action(str(state), avail_action)

            new_state, reward, done = env.step(action, state, step)

            RL.learn(str(state), action, reward, str(new_state))
            state = dict(new_state)

            if done:
                break
            step += 1
        if done:
            print(state)
            break
    return done


def get_mapping(graph, pre):
    i = 1
    mapping = {}
    for node in graph:
        if "execve" in node:
            mapping[node] = f"{pre}_0"
        else:
            mapping[node] = f"{pre}_{i}"
            i += 1
    return mapping


def RL(path):
    base = os.path.basename(path)
    G_name = os.path.splitext(base)[0]
    G = nx.read_adjlist(path, create_using=nx.DiGraph)
    G = nx.relabel_nodes(G, mapping=get_mapping(G, "mal"))

    beg_path = np.random.choice(glob("data/dataset/benign/*.adjlist"))
    H = nx.read_adjlist(beg_path, create_using=nx.DiGraph)
    H = nx.relabel_nodes(H, mapping=get_mapping(H, "beg"))

    v = list(G.neighbors("mal_0"))[0]
    G.add_edge("mal_0", "fork")
    G.add_edge("fork", v)
    G.add_edge("fork", "beg_0")
    G.remove_edge("mal_0", v)

    edges = deepcopy(H.edges)
    for (u, v) in edges:
        G.add_edge(u, v)

    actions = []
    nodes = deepcopy(H.nodes)
    for node in nodes:
        if (node, node) not in H.edges:
            actions.append(node)
    env = Environment(graph=G)
    RL = QLearningTable(actions=actions)

    n_eps = 10
    done = update(n_eps, env, RL)
    if not done:
        print("Attack failed.")
    else:
        shutil.copy("data/dataset/AE/AE.adjlist", f"data/AE/AE_{G_name}.adjlist")


if __name__ == '__main__':
    paths = glob("data/dataset/malware/*.adjlist")
    for i, path in enumerate(paths[132:]):
        print(i, path)
        RL(path)
