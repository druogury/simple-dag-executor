# coding: utf-8
import pandas as pd

import networkx as nx
from networkx.algorithms.dag import (transitive_reduction, is_directed_acyclic_graph)


#%%
def build_dag(fname: str):
    """ build DAG with Networkx

    Parameters
    ----------
    fname : file containing dependencies
    query    parents
    req3.sql req1.sql,req2.sql

    Returns
    -------
    G : directed graph
    """
    df = pd.read_csv(fname, sep=' ', names=["node", "parents"], header='infer')

    # build DAG
    G = nx.DiGraph()
    for idx, row in df.iterrows():
        node = row["node"]
        for parent in row["parents"].split(','):
            G.add_edge(parent, node)

    # reduce graph
    G = transitive_reduction(G)

    assert is_directed_acyclic_graph(G), "check it is a DAG"

    return G

