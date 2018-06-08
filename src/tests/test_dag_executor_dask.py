# coding: utf-8
import time
from dask.multiprocessing import get

from project import *
from src.dag_executor_dask import *


def test_read_exec_query():
    cmd = read_query(TST_TXT_DIR + 'sleep1.txt')
    status = exec_query(cmd)
    assert int(status) == 0


# does not work : exec needs to return next query, not the status
"""
def test_dag_executor_dask():
    
    dsk = {
        'read-1': (read_query, TST_TXT_DIR + 'sleep2.txt'),
        'read-2': (read_query, TST_TXT_DIR + 'sleep3.txt'),
        'read-3': (read_query, TST_TXT_DIR + 'sleep1.txt'),
        'read-4': (read_query, TST_TXT_DIR + 'sleep9.txt'),            
        'exec-1': (exec_query, 'read-1'),
        'exec-2': (exec_query, 'exec-1'),
        'exec-4': (exec_query, 'read-4'),
        'exec-3': (exec_query, ['exec-2', 'exec-4'])
    }

    t0 = time()
    get(dsk, 'exec-3')
    t1 = time()
    print("run time :", t1 - t0)

    assert 4 <= t1 - t0 < 5
"""
