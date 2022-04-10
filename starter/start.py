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


def get_db_connection_variables(config_file):
    if not exists(config_file):
        config_file = input(
            "The file \"{0}\" does not exist. Please provide the path of the config file: ".format(config_file))

        return get_db_connection_variables(config_file)
    else:
        file_read = open(config_file, "r")

        connection_string = ""

        for line in file_read.readlines():
            if "mysql_connection_string" in line:
                connection_string = re.search(".*\ \"(.*)\"", line).group(1)
                break

        file_read.close()

        variable_strings = connection_string.split(";")
        variable_dict = dict(s.split('=') for s in variable_strings)

        return variable_dict


def get_db(connection_variables):
    db_connection = mysql.connector.connect(
        host=connection_variables['server'],
        user=connection_variables['userid'],
        password=connection_variables['password'],
        database=connection_variables['database']
    )
    db_cursor = db_connection.cursor()

    return db_connection, db_cursor


def execute_sql_scripts():
    db_connection = None

    print("Waiting for database")
    while not db_connection:
        try:
            db_configs = get_db_connection_variables("./cfx-server-data/server.cfg")
            db_connection, db_cursor = get_db(db_configs)
        except Exception as e:
            time.sleep(1)
    print("Database started")

    db_cursor.execute("CREATE TABLE IF NOT EXISTS `ku_sql_files` (`sql_file` varchar(255) COLLATE utf8mb4_bin NOT NULL, PRIMARY KEY (`sql_file`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")

    db_cursor.execute("SELECT * FROM `ku_sql_files`;")
    results = db_cursor.fetchall()
    existing_files = []
    for result in results:
        existing_files.append(result[0])

    sql_scripts_paths = []

    path_list = Path("./cfx-server-data/resources").glob('**/*.sql')
    for path in path_list:
        sql_scripts_paths.append(str(path))

    use_db_command = "USE {0}; ".format(db_configs['database'])
    table_command_list = {}
    other_command_list = {}

    for path in sql_scripts_paths:
        relative_path = re.search("resources.*", path)[0]

        if relative_path.replace("\\", "") not in existing_files:
            fd = open(path, 'r')
            commands = fd.read()
            fd.close()

            table_command_list[relative_path] = []
            other_command_list[relative_path] = []

            for command in commands.split(';'):
                command = command.strip().replace("\n", " ").replace("\t", " ")
                if command and "use " not in command.lower() and "create database " not in command.lower():
                    if "create table " in command.lower():
                        table_command_list[relative_path].append(command)
                    else:
                        other_command_list[relative_path].append(command)

    try:
        db_cursor.execute(use_db_command)

        for file in table_command_list:
            print("Creating tables from file {0}".format(file))
            for command in table_command_list[file]:
                try:
                    db_cursor.execute(command)
                except Exception as e:
                    if e.errno == 1050:
                        print(
                            "\033[1;33mWARNING:\033[0;37m \"{0}\" when executing \"{1}...\" in file \"{2}\" - We continue the process".format(
                                e.msg, command[0:30], file))

        for file in other_command_list:
            print("Inserting data from file {0}".format(file))
            try:
                for command in other_command_list[file]:
                    db_cursor.execute(command)

                db_cursor.execute("INSERT INTO ku_sql_files VALUES ('{0}')".format(file))
            except Exception as e:
                print("\033[1;31mERROR:\033[0;37m\"{0}\" when executing \"{1}...\" it the file \"{2}\"".format(e.msg, command[0:30],file))
                raise Exception()

        db_connection.commit()
    except Exception as e:
        print(
            "\033[1;31m\nWe rollback and stop the execution. Make sure you have all the resources dependencies.\033[0;37m")
        db_connection.rollback()


if not exists("./server"):
    print('Download Server')

    opener = urllib.request.URLopener()
    opener.addheader('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36')
    filename, headers = opener.retrieve("https://runtime.fivem.net/artifacts/fivem/build_server_windows/master/5402-810a639673d8da03fe4b1dc2b922c9c0265a542e/server.7z", './server.7z')

    archive = py7zr.SevenZipFile('./server.7z', mode='r')
    archive.extractall(path="./server")
    archive.close()

    os.remove("./server.7z")

repo = Repo("./")
for submodule in repo.submodules:
    submodule.update(init=True)

repo = Repo("./cfx-server-data")
for submodule in repo.submodules:
    submodule.update(init=True)

print('Initiate Database')
os.system('docker-compose up -d')

print('Executing SQL scripts')
execute_sql_scripts()

print('Start Server')
subprocess.Popen(r"./server/FXServer.exe +exec server.cfg", cwd=r"./cfx-server-data")

input("Completed [ENTER] to exit")

exit(0)