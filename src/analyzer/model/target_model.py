import os
import numpy as np
import pandas as pd
from joblib import dump
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn import metrics
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC

SEED = 42
DATA_PATH = "data/embeddings.csv"
REPORT_PATH = "result/target_model.txt"

if os.path.exists(REPORT_PATH):
    os.remove(REPORT_PATH)

data = pd.read_csv(DATA_PATH)
X, y, X_new = [], [], []
for row in data.values:
    if os.path.exists(f"data/dataset/benign/{row[0]}.adjlist"):
        X.append(row[1:])
        y.append(0)
    if os.path.exists(f"data/dataset/malware/{row[0]}.adjlist"):
        X.append(row[1:])
        y.append(1)
    if os.path.exists(f"data/dataset/new_malware/{row[0]}.adjlist"):
        X_new.append(row[1:])

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=.3, random_state=SEED, stratify=y)
X_test.extend(X_new)
y_test.extend([1] * len(X_new))
print(len(X_train), len(X_test))

scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)
X_new = scaler.transform(X_new)
dump(scaler, "model/scaler.joblib")


classifiers = {
    "NB": GaussianNB(),
    "DT": DecisionTreeClassifier(random_state=SEED, class_weight="balanced"),
    "KNN": KNeighborsClassifier(n_jobs=-1),
    "SVM": SVC(random_state=2020, probability=True, class_weight="balanced"),
    "RF": RandomForestClassifier(random_state=SEED, class_weight="balanced", n_jobs=-1),
}

hyperparam = [
    {},
    {"criterion": ["gini", "entropy"]},
    {"n_neighbors": [5, 100, 500], "weights": ["uniform", "distance"]},
    {"C": np.logspace(-3, 3, 7), "gamma": np.logspace(-3, 3, 7)},
    {"criterion": ["gini", "entropy"], "n_estimators": [10, 100, 1000]},
]

for (name, est), hyper in zip(classifiers.items(), hyperparam):
    clf = GridSearchCV(est, hyper, cv=5, n_jobs=-1)

    clf.fit(X_train, y_train)

    y_prob = clf.predict_proba(X_test)
    y_pred = clf.predict(X_test)
    y_pred_new = clf.predict(X_new)

    dump(clf, f"model/{name}.joblib")

    with open(REPORT_PATH, 'a') as f:
        clf_rp = metrics.classification_report(y_test, y_pred, digits=4)
        cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
        AUC = metrics.roc_auc_score(y_test, y_prob[:, 1])
        TN, FP, FN, TP = cnf_matrix.ravel()
        FPR = FP / (FP + TN)
        f.write(f"{name}\n")
        f.write(str(clf_rp) + '\n')
        f.write(str(cnf_matrix) + '\n\n')
        f.write(f"ROC AUC: {round(AUC, 4)}\n")
        f.write(f"FRP: {round(FPR, 4)}\n")
        f.write(f"Detect new malwares: {y_pred_new.sum()}/{len(X_new)}\n")
        f.write('-' * 88 + '\n')
