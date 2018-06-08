# coding: utf-8
import os
import pwd
import socket

HOST = socket.gethostname()
USER = pwd.getpwuid(os.getuid())[0]

print("host :", HOST)
print("user :", USER)

if HOST == "Baptistes-MacBook-Pro.local" and USER == "admin" :  # MAC
    ROOT_DIR = "/Users/{}/proj/simple-dag-executor/".format(USER)
elif HOST.startswith("ip-172-31-") and USER == "drussier":  # VM EC2
    ROOT_DIR = "/home/{}/proj/simple-dag-executor/".format(USER)

SRC_DIR = ROOT_DIR + "src/"

TST_DIR = SRC_DIR + "tests/"
TST_TXT_DIR = TST_DIR + "txt/"
TST_SQL_DIR = TST_DIR + "sql/"
TST_BASH_DIR = TST_DIR + "bash/"

PARAM_FILE = ROOT_DIR + "parameters.ini"

if __name__ == "__main__":
    print(HOST)
    print(USER)
    print(ROOT_DIR)
    print(SRC_DIR)
    print(TST_DIR)
