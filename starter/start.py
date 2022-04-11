# https://runtime.fivem.net/artifacts/fivem/build_server_windows/master/5402-810a639673d8da03fe4b1dc2b922c9c0265a542e/server.7z

# pip install mysql-connector-python

# from contextlib import nullcontext
# from sre_constants import NOT_LITERAL

import mysql.connector
import re
import os
import time
import py7zr
import urllib.request
import subprocess

from git import Repo
from pathlib import Path
from os.path import exists

config_file = "./server.cfg"


def get_enabled_modules():
    file_read = open("./server.cfg", "r")
    modules = re.findall("\n\s*ensure\s([^\s]*)", file_read.read())
    file_read.close()

    return modules


def get_script_fragments(path):
    elements = path.split("\\")

    name = elements.pop()
    module = ""
    groups = []

    elements = elements[2:]

    for element in elements:
        if bool(re.search("\[.*\]", element)):
            groups.append(element)
        else:
            module = element
            break

    return {
        'module': module,
        'name': name,
        "groups": groups,
        "path": path
    }


def get_sql_scripts():
    enabled_modules_scripts = {key: [] for key in get_enabled_modules()}

    path_list = Path("./server-data/resources").glob('**/*.sql')
    for path in path_list:
        script_fragments = get_script_fragments(str(path))

        for fragment in script_fragments['groups']:
            if fragment in enabled_modules_scripts:
                script_fragments['queries'] = split_sql_script_queries(script_fragments['path'])
                enabled_modules_scripts[fragment].append(script_fragments)
                break

        if script_fragments['module'] in enabled_modules_scripts:
            script_fragments['queries'] = split_sql_script_queries(script_fragments['path'])
            enabled_modules_scripts[script_fragments['module']].append(script_fragments)

    return {k: v for k, v in enabled_modules_scripts.items() if v}


def split_sql_script_queries(path):
    queries = {
        'create': [],
        'alter': [],
        'insert': []
    }

    script_content = ""
    fd = open(path, encoding="utf8")
    for line in fd.readlines():
        if not re.search("--.*$", line.strip()):
            script_content = script_content + line.replace("\n", " ").replace("\t", " ")
    fd.close()

    for query in script_content.split(';'):
        if query.strip() != '' and \
           "use " not in query.lower() and\
           "create database " not in query.lower() and\
           "alter database " not in query.lower():

            if "create table " in query.lower():
                queries['create'].append(query)
            elif "alter table " in query.lower():
                queries['alter'].append(query)
            else:
                queries['insert'].append(query)

    return queries


def get_db_connection_variables(config_file):
    file_read = open(config_file, "r")
    content = file_read.read()
    file_read.close()
    connection_string = re.search("mysql:\/\/(.*):(.*)@(.*)\/(.*)\?", content)

    return {
        'user': connection_string.group(1),
        'password': connection_string.group(2),
        'host': connection_string.group(3),
        'database': connection_string.group(4)
    }


def get_db():
    connection_variables = get_db_connection_variables("./server.cfg")

    db_connection = None
    db_cursor = None

    print("Waiting for database")
    while not db_connection:
        try:
            db_connection = mysql.connector.connect(
                host=connection_variables['host'],
                user=connection_variables['user'],
                password=connection_variables['password'],
                database=connection_variables['database']
            )
            db_cursor = db_connection.cursor()
        except Exception as e:
            time.sleep(1)
    print("Database started")

    return db_connection, db_cursor, connection_variables


def execute_sql_queries(queries, db_cursor):
        sql_error_query_logs = ""

        for query in queries:
            try:
                db_cursor.execute(query)
            except Exception as e:
                if e.errno in [1050, 1060, 1062, 1136]:
                    print("\033[1;33mWARNING:\033[0;37m \"{0}\" when executing \"{1}...\"".format(e.msg, query[0:30]))

                    if e.errno != 1050:
                        sql_error_query_logs += "\n{0}\n{1}\n".format(e.msg, query.strip())
                else:
                    print("\033[1;31mERROR:\033[0;37m\"{0}\" when executing \"{1}...\"".format(e.msg, query[0:30]))
                    print("\033[1;31m\nWe rollback and stop the execution. Make sure you have all the resources dependencies.\033[0;37m")

                    raise e

        return sql_error_query_logs


def execute_sql_scripts():
    db_connection, db_cursor, db_configs = get_db()

    db_cursor.execute("CREATE DATABASE IF NOT EXISTS `{0}`;".format(db_configs['database']))

    db_cursor.execute("USE {0}; ".format(db_configs['database']))
    db_cursor.execute("CREATE TABLE IF NOT EXISTS `ku_sql_files` (`sql_file` varchar(255) COLLATE utf8mb4_bin NOT NULL, PRIMARY KEY (`sql_file`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")

    db_cursor.execute("SELECT * FROM `ku_sql_files`;")
    existing_files = [files[0] for files in db_cursor.fetchall()]

    create_queries = []
    alter_queries = []
    insert_queries = []

    for key, scripts in get_sql_scripts().items():
        for script in scripts:
            if script['path'] in existing_files:
                continue

            # TODO: Add alter database
            create_queries = create_queries + script['queries']['create']
            alter_queries = alter_queries + script['queries']['alter']
            insert_queries = insert_queries + script['queries']['insert']

    try:
        error_messages = ""
        error_messages += execute_sql_queries(create_queries, db_cursor)
        error_messages += execute_sql_queries(alter_queries, db_cursor)
        error_messages += execute_sql_queries(insert_queries, db_cursor)

        if error_messages != "":
            print("\n\033[1;31mSome queries have not been executed correctly. We've created a file with all the problematic queies [sql_error_query.log].\nPlease review it to make sure everythig works as expected.\nIf a query didn't run and should have been, please make the correction and execute it manually\033[0;37m")

            if os.path.exists("sql_error_query.log"):
                os.remove("sql_error_query.log")

            f = open("sql_error_query.log", "a", encoding="utf-8")
            f.write(error_messages)
            f.close()

    except Exception as e:
        db_connection.rollback()

    db_connection.commit()


if not exists("./server"):
    print('Download Server')

    opener = urllib.request.URLopener()
    opener.addheader('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36')
    filename, headers = opener.retrieve("https://runtime.fivem.net/artifacts/fivem/build_server_windows/master/5402-810a639673d8da03fe4b1dc2b922c9c0265a542e/server.7z", './server.7z')

    archive = py7zr.SevenZipFile('./server.7z', mode='r')
    archive.extractall(path="./server")
    archive.close()

    os.remove("./server.7z")

print("Refresh modules")
repo = Repo("./")
for submodule in repo.submodules:
    submodule.update(init=True, recursive=True)

repo = Repo("./server-data")
for submodule in repo.submodules:
    submodule.update(init=True)

print('Initiate Database')
os.system('docker-compose up -d')

print('Executing SQL scripts')
execute_sql_scripts()

print('Start Server')
subprocess.Popen(r"./server/FXServer.exe +exec ../server.cfg", cwd=r"./server-data")

input("Completed [ENTER] to exit")
