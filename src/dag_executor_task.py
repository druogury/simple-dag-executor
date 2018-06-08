# coding: utf-8
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import time
import networkx as nx

from configparser import SafeConfigParser
parser = SafeConfigParser()

from project import *
from src.dag import build_dag


class Task:
    """ single task
    """
    def __init__(self, name, parents, func):
        self.name = name
        self.parents = parents if parents else []
        self.children = []
        self.func = func
        # nb of parents
        self.unresolved_dependencies = len(parents) if parents else 0
        self.unresolved_dependencies_list = parents if parents else []
        self.submitted = False
        self.completed = False

    def resolve_dependency(self, parent):
        """ decrement counter of parents
        """
        if self.parents and parent in self.parents:
            if self.unresolved_dependencies > 0:
                self.unresolved_dependencies -= 1
            if self.unresolved_dependencies_list and parent in self.unresolved_dependencies_list:
                self.unresolved_dependencies_list.remove(parent)

    def __repr__(self):
        return "\n" + "**********" + "\n" \
            "name          : '" + self.name + "'\n" \
            "parents       : " + str([p.name for p in self.parents]) + "\n" \
            "children      : " + str([c.name for c in self.children]) + "\n" \
            "nb of parents : " + str(self.unresolved_dependencies) + "\n" \
            "submitted     : " + str(self.submitted) + "\n" \
            "completed     : " + str(self.completed) + "\n" \
            "**********" + "\n"


class TaskDag:
    """ DAG of tasks
    """
    def __init__(self):
        self.tasks = {}
        self.G = nx.DiGraph()
        pass

    def add_task(self, name, func, parents=None):
        """ assume parents of current task are known already
        """
        # check task is not in the graph already
        assert name not in self.tasks

        # build list of parent tasks form parent names
        if parents:
            parents = [self.tasks[p] for p in parents]

        # build task with name and parent tasks
        task = Task(name, parents, func)

        # update children = add task to every child
        if parents:
            for p in parents:
                p.children.append(task)

        # put task in graph
        self.tasks[name] = task    

    def build(self, fname: str):
        self.G = build_dag(fname)
        return self



class TaskDagExecutor:

    def __init__(self, max_workers=None):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def execute(self, task_dag):
        # copy list of tasks
        tasks = deepcopy(list(task_dag.tasks.values()))
        self._execute_ready_tasks(tasks)

        # https://stackoverflow.com/questions/21143162/python-wait-on-all-of-concurrent-futures-threadpoolexecutors-futures
        # self.executor.shutdown(wait=True)

    def _execute_ready_tasks(self, tasks):
        # tasks ready to be executed : not submitted yet and no parents
        ready_tasks = list(
            filter(
                # https://docs.python.org/3/reference/expressions.html#operator-precedence
                lambda t: (not t.submitted) and (t.unresolved_dependencies == 0), tasks                
            )
        )

        # update list of tasks
        tasks = [t for t in tasks if t not in ready_tasks]

        # run ready tasks
        for t in ready_tasks:
            try:
                print("{} submitted at {}".format(t.name, time.time()))
                t.submitted = True
                future = self.executor.submit(t.func)
                future.add_done_callback(
                    lambda f: self._clear_completed_task(f, t, tasks))
            except Exception as e:
                print(e)
                t.submitted = False

    def _clear_completed_task(self, future, task, tasks):
        if future.done():
            print("{} completed at {}".format(task.name, time.time()))            
            task.completed = True
        for c in task.children:
            c.resolve_dependency(task)
        self._execute_ready_tasks(tasks)



        
if __name__ == "__main__":
    # test1()

    # t0 = time.time()
    test4()
    # t1 = time.time()
    # print("run time :", t1 - t0)
