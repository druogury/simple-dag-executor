# coding: utf-8
from time import time

import ipyparallel as ipp
import networkx as nx

from configparser import SafeConfigParser
parser = SafeConfigParser()

from src.dag import build_dag

#%%
class TaskDAG():

    def __init__(self, run_job_fn, sql_dir: str):
        # init empty graph
        self.G = nx.DiGraph()
        self.run_job_fn = run_job_fn
        self.sql_dir = sql_dir

    def build(self, fname: str):
        self.G = build_dag(fname)
        return self

    def execute(self, connector=None):
        jobs = {}
        for node in self.G:
            query = open(self.sql_dir + node, 'r').read()
            if connector is None:
                jobs[node] = lambda : self.run_job_fn(query)
            else:
                jobs[node] = lambda : self.run_job_fn(query, connector)

        rc = ipp.Client()
        view = rc.load_balanced_view()

        results = {}
        for node in nx.topological_sort(self.G):
            # get list of AsyncResult objects from nodes
            # leading into this one as dependencies
            deps = [ results[n] for n in self.G.predecessors(node) ]
            # submit and store AsyncResult object
            with view.temp_flags(after=deps, block=False):
                results[node] = view.apply(jobs[node])

        self.results = results
        
        # Now that we have submitted all the jobs, we can wait for the results
        view.wait(self.results.values())

        # self.check_tree(results)

    def check_tree(self):
        """Validate that jobs executed after their dependencies."""
        for node in self.G:
            started = self.results[node].metadata.started
            for parent in self.G.predecessors(node):
                finished = self.results[parent].metadata.completed
                assert started > finished, "{} should have happened after {}".format(
                    node, parent)        

            
if __name__ == '__main__':
    parser.read('parameters.ini')
    DBNAME   = parser.get("redshift", "dbname")
    HOST     = parser.get("redshift", "host")
    PASSWORD = parser.get("redshift", "password")
    PORT     = parser.get("redshift", "port")
    USER     = parser.get("redshift", "user")
    connector = open_connection(DBNAME, USER, HOST, PORT, PASSWORD)

    if False:
        query = "SELECT * from apps LIMIT 5 ;"
        run_query_py(query, connector)
        
    if True:
        pass


    close_connection(connector)    
    
