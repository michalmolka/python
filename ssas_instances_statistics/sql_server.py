"""Looks for SQLServer instances, returns connections data."""
import subprocess
from sys import path

path.append("\\Program Files\\Microsoft.NET\\ADOMD.NET\\150")
from pyadomd import Pyadomd


class SQLServer:
    """Looking for SQL Server Instances and databases."""

    sql_server_instances_list = []
    instance_database_list = []
    sql_server_connection_string_list = []

    def __str__(self):
        return "SQL Server"

    @classmethod
    def sql_server_instances(cls):
        """Looks for SQL Server instance names."""

        instance_name = subprocess.run(  # Gets active instances.
            "powershell.exe Get-Service | Where-Object {$_.Name -like 'MSSQL$*'}",
            stdout=subprocess.PIPE,
            universal_newlines=True,
            stderr=subprocess.PIPE,
            check=False,
        ).stdout
        if not instance_name:
            return
        # Looks for instance names.
        first_letter = [i for i, letter in enumerate(instance_name) if letter == "("]
        last_letter = [i for i, letter in enumerate(instance_name) if letter == ")"]
        letters = list(zip(first_letter, last_letter))
        sql_server_instances = [instance_name[x[0] + 1 : x[1]] for x in letters]
        cls.sql_server_instances_list = sql_server_instances
        # return sql_server_instances
        return SQLServer()

    @classmethod
    def sql_server_databases(cls, instances):
        """Returns instances and databases"""

        if not instances:
            return
        databases = []
        for instance in instances:  # For every instance looks for databses.
            connection_string = r"Provider=MSOLAP;Data Source=.\{0};".format(instance)
            query_string = """select [DATABASE_ID] from $SYSTEM.DBSCHEMA_CATALOGS"""
            with Pyadomd(connection_string) as conn:
                with conn.cursor().execute(query_string) as cur:
                    dataset = cur.fetchall()
            if dataset:
                databases.append([database[0] for database in dataset])
            elif not dataset:
                raise Exception("No SSAS instances.")
        instance_database = list(zip(instances, databases))
        cls.instance_database_list = instance_database
        return SQLServer()

    @classmethod
    def sql_server_connection_string(cls):
        """Returns connection strings"""

        for instance_database in cls.instance_database_list:
            for database in instance_database[1]:
                cls.sql_server_connection_string_list.append(
                    r"Provider=MSOLAP;Data Source=.\{0}".format(instance_database[0])
                    + ";Catalog="
                    + database
                    + ";"
                )
        return SQLServer()
