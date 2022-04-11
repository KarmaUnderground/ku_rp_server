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
        'name': name,
        'module': module,
        "groups": groups
    }


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


def get_db(connection_variables):
    db_connection = mysql.connector.connect(
        host=connection_variables['host'],
        user=connection_variables['user'],
        password=connection_variables['password'],
        database=connection_variables['database']
    )
    db_cursor = db_connection.cursor()

    return db_connection, db_cursor


def execute_sql_scripts():
    db_connection = None

    enabled_modules = get_enabled_modules()

    print("Waiting for database")
    while not db_connection:
        try:
            db_configs = get_db_connection_variables("./server.cfg")
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

    path_list = Path("./server-data/resources").glob('**/*.sql')
    for path in path_list:
        sql_scripts_paths.append(str(path))

    use_db_command = "USE {0}; ".format(db_configs['database'])
    create_command_list = {}
    alter_command_list = {}
    other_command_list = {}

    for path in sql_scripts_paths:
        script_fragments = get_script_fragments(path)

        in_group = False
        for fragment in script_fragments['groups']:
            if fragment in enabled_modules:
                in_group = True
                break

        if not in_group and script_fragments['module'] not in enabled_modules:
            continue

        relative_path = re.search("resources.*", path)[0]

        if relative_path.replace("\\", "") not in existing_files:
            try:
                commands = ""
                fd = open(path, encoding="utf8")
                for line in fd.readlines():
                    if not re.search("--.*$", line.strip()):
                        commands = commands + line.replace("\n", " ").replace("\t", " ")
                # commands = fd.read()
                fd.close()
            except:
                pass

            create_command_list[relative_path] = []
            alter_command_list[relative_path] = []
            other_command_list[relative_path] = []

            for command in commands.split(';'):
                command = command.strip().replace("\n", " ").replace("\t", " ")

                if command and "use " not in command.lower() and "create database " not in command.lower() and "alter database " not in command.lower():
                    if "create table " in command.lower():
                        create_command_list[relative_path].append(command)
                    elif "alter table " in command.lower():
                        alter_command_list[relative_path].append(command)
                    else:
                        other_command_list[relative_path].append(command)

            if len(create_command_list[relative_path]) == 0:
                del(create_command_list[relative_path])
            if len(alter_command_list[relative_path]) == 0:
                del(alter_command_list[relative_path])
            if len(other_command_list[relative_path]) == 0:
                del(other_command_list[relative_path])

    try:
        db_cursor.execute(use_db_command)

        for file in create_command_list:
            print("Creating tables from file {0}".format(file))
            for command in create_command_list[file]:
                try:
                    db_cursor.execute(command)

                except Exception as e:
                    if e.errno == 1050:
                        print(
                            "\033[1;33mWARNING:\033[0;37m \"{0}\" when executing \"{1}...\" in file \"{2}\" - We continue the process".format(
                                e.msg, command[0:30], file))

        for file in alter_command_list:
            print("Alter table from file {0}".format(file))
            try:
                for command in alter_command_list[file]:
                    db_cursor.execute(command)

                db_cursor.execute("INSERT IGNORE INTO ku_sql_files VALUES ('{0}')".format(file))
            except Exception as e:
                print("\033[1;31mERROR:\033[0;37m\"{0}\" when executing \"{1}...\" it the file \"{2}\"".format(e.msg, command[0:30],file))
                raise Exception()

        for file in other_command_list:
            print("Inserting data from file {0}".format(file))
            try:
                for command in other_command_list[file]:
                    db_cursor.execute(command)

                db_cursor.execute("INSERT IGNORE INTO ku_sql_files VALUES ('{0}')".format(file))
            except Exception as e:
                print("\033[1;31mERROR:\033[0;37m\"{0}\" when executing \"{1}...\" it the file \"{2}\"".format(e.msg, command[0:30],file))
                raise Exception()

        db_connection.commit()
    except Exception as e:
        print(
            "\033[1;31m\nWe rollback and stop the execution. Make sure you have all the resources dependencies.\033[0;37m")
        db_connection.rollback()
        exit(0)


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

# input("Completed [ENTER] to exit")
