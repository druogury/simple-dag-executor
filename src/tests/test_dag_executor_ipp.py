# coding: utf-8
import os
from networkx.readwrite import json_graph

from project import *
from src.dag_executor_ipp import *
from src.aws_redshift_connect import *


def run_bash_cmd(cmd):
    os.system(cmd)


def test_TaskDAG1():
    # TypeError: can't pickle psycopg2.extensions.connection objects
    """
    parser.read(PARAM_FILE)
    DBNAME   = parser.get("redshift", "dbname")
    HOST     = parser.get("redshift", "host")
    PASSWORD = parser.get("redshift", "password")
    PORT     = parser.get("redshift", "port")
    USER     = parser.get("redshift", "user")
    connector = open_connection(DBNAME, USER, HOST, PORT, PASSWORD)

    dag = TaskDAG(run_query_py, TST_SQL_DIR)
    print(dag.sql_dir)
    dag.build(TST_SQL_DIR + "dependencies.txt")
    print(json_graph.node_link_data(dag.G)['links'])
    print([n for n in dag.G])
    dag.execute(connector)
    """
    pass

    
def test_TaskDAG2():
    dag = TaskDAG(run_bash_cmd, TST_BASH_DIR)
    dag.build(TST_BASH_DIR + "dependencies.txt")
    # print(json_graph.node_link_data(dag.G)['links'])
    # print([n for n in dag.G])
    dag.execute()
