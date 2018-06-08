# coding: utf-8
import os


def run_bash_cmd(cmd):
    status = os.system(cmd)
    return status
