import os
import time

from project import *
from src.dag_executor_task import *
from src.utils_aws_redshift import *

        
def test1():
    from random import random

    task_dag = TaskDag()
    task_dag.add_task("1 + 2", lambda: print(1 + 2))
    task_dag.add_task("10 + 20", lambda: print(10 + 20))
    task_dag.add_task("4 * 3 + 20", lambda: print(4 * 3 + 20), ["1 + 2", "10 + 20"])
    task_dag.add_task("15  / 5", lambda: print(15 / 5), ["4 * 3 + 20"])
    task_dag_executor = TaskDagExecutor(10)
    task_dag_executor.execute(task_dag)

    time.sleep(5)

    
def test2():
    """ 1 (2s) -> 2 (3s) = 5s
    """
    task_dag = TaskDag()
    task_dag.add_task("1 : sleep 2", lambda: time.sleep(2))
    task_dag.add_task("2 : sleep 3", lambda: time.sleep(3))    

    task_dag_executor = TaskDagExecutor(10)
    task_dag_executor.execute(task_dag)


def test3():
    """ 1 (2s) -> 2 (3s) -> 3 (9s) = 15s
                  4 (6s) ->/        
    """
    task_dag = TaskDag()
    task_dag.add_task("1 : sleep 2", lambda: time.sleep(2))
    task_dag.add_task("2 : sleep 3", lambda: time.sleep(3))
    task_dag.add_task("4 : sleep 6", lambda: time.sleep(6))
    task_dag.add_task("3 : sleep 9", lambda: os.system("sleep 9s && echo done"), # time.sleep(9),
                      ["2 : sleep 3", "4 : sleep 6"])        

    print(task_dag.tasks)
    # stop
    
    task_dag_executor = TaskDagExecutor(10)
    task_dag_executor.execute(task_dag)


def test4(sql_dir: str=TST_SQL_DIR):
    parser.read(PARAM_FILE)
    DBNAME   = parser.get("redshift", "dbname")
    HOST     = parser.get("redshift", "host")
    PASSWORD = parser.get("redshift", "password")
    PORT     = parser.get("redshift", "port")
    USER     = parser.get("redshift", "user")
    connector = open_connection(DBNAME, USER, HOST, PORT, PASSWORD)
    
    task_dag = TaskDag()
    task_dag.build(sql_dir + "dependencies.txt")
    for node in nx.topological_sort(task_dag.G):
        query = open(sql_dir + node, 'r').read()
        print(node)
        print(query)
        task_dag.add_task(
            node,
            lambda: run_query_py(query, connector), task_dag.G.predecessors(node)
        )
    print(task_dag.tasks)
    
    task_dag_executor = TaskDagExecutor(10)
    task_dag_executor.execute(task_dag)


        
if __name__ == "__main__":
    # test1()

    # t0 = time.time()
    test4()
    # t1 = time.time()
    # print("run time :", t1 - t0)
