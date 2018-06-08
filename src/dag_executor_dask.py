# coding: utf-8
import os


def read_query(fname):
    with open(fname, 'r') as f:
        return f.read()


def exec_query(query):
    """ simple bash for now """

    status = os.system(query)
    # print("query : {} | status : {}".format(query, status))
    # int not possible

    return str(status)
