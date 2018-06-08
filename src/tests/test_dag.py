# coding: utf-8
import pytest
import networkx as nx
from networkx.algorithms.dag import transitive_reduction
from networkx.readwrite import json_graph
from networkx.exception import NetworkXError

from configparser import SafeConfigParser
parser = SafeConfigParser()

from project import *
from src.dag import *


def test_transitive_reduction():
    # reduce
    # 1 -> 2 -> 3
    # |         |
    #  ---->----
    # in
    # 1 -> 2 -> 3
    
    G = nx.DiGraph()
    G.add_edges_from([(1, 2), (2, 3), (1, 3)])
    G1 = transitive_reduction(G)

    assert json_graph.node_link_data(G1)['links'] == [{'source': 1, 'target': 2}, {'source': 2, 'target': 3}]

    # reduce 
    #  <- 1 ->
    # |       |
    # 2 --<-- 4
    # |       |
    #  -> 3 ->
    # error : loop
    
    G = nx.DiGraph()
    G.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 4), (4, 2)])

    with pytest.raises(NetworkXError) as excinfo:
        G1 = transitive_reduction(G)
        assert 'Transitive reduction only uniquely defined on directed acyclic graphs' in str(excinfo.value)



def test_build_dag():
    G = build_dag(TST_SQL_DIR + "dependencies.txt")
    links = [
        {'source': 'req1.sql', 'target': 'req2a.sql'},
        {'source': 'req1.sql', 'target': 'req2b.sql'},
        {'source': 'req2a.sql', 'target': 'req3.sql'},
        {'source': 'req2b.sql', 'target': 'req3.sql'},
        {'source': 'req4.sql', 'target': 'req2b.sql'}
    ]

    # list of dict cannot be converted into set of dict (non hashable type)
    assert \
        set([ (l["source"], l["target"]) for l in json_graph.node_link_data(G)['links'] ]) == \
        set([ (l["source"], l["target"]) for l in links ])
