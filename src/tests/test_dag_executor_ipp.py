# coding: utf-8
import time
from networkx.readwrite import json_graph

from project import *
from src.dag_executor_ipp import *
from src.utils_aws_redshift import *
from src.utils_bash import run_bash_cmd


def test_TaskDAG_redshift():
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

    
def test_TaskDAG_bash():
    dag = TaskDAG(run_bash_cmd, TST_BASH_DIR)
    dag.build(TST_BASH_DIR + "dependencies.txt")
    # print(json_graph.node_link_data(dag.G)['links'])
    # print([n for n in dag.G])
    dag.execute()
    time.sleep(10)
    dag.check_tree()
