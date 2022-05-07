import numpy as np
import pandas as pd


class QLearningTable:
    def __init__(self, actions, learning_rate=0.9, reward_decay=0.9, e_greedy=0.8):
        self.actions = actions
        self.lr = learning_rate
        self.gamma = reward_decay
        self.epsilon = e_greedy

        self.q_table = pd.DataFrame(columns=self.actions, dtype=np.float64)

    def get_all_actions(self):
        return self.actions

    def get_q_table(self):
        return self.q_table

    def choose_action(self, state, all_avail_actions):
        self.check_state_exist(state)
        avail_actions = []
        for action in all_avail_actions:
            if action not in state:
                avail_actions.append(action)

        if np.random.uniform() > self.epsilon:
            state_action = self.q_table.loc[state, avail_actions]
            state_action = state_action.reindex(
                np.random.permutation(state_action.index))
            action = state_action.idxmax()
            if self.q_table.loc[state, action] == 0:
                action = np.random.choice(avail_actions)
        else:
            action = np.random.choice(avail_actions)

        return action

    def learn(self, state, action, reward, next_state):
        self.check_state_exist(next_state)

        q_predict = self.q_table.loc[state, action]
        q_target = reward + self.gamma * self.q_table.loc[next_state, :].max()
        self.q_table.loc[state, action] += self.lr * (q_target - q_predict)

        return self.q_table.loc[state, action]

    def check_state_exist(self, state):
        if state not in self.q_table.index:
            self.q_table = self.q_table.append(
                pd.Series(
                    [0]*len(self.actions),
                    index=self.q_table.columns,
                    name=state,
                )
            )
