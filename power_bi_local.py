"""
Looks for local Power BI instances.
"""
import re
import os
from sys import path
import psutil
from more_itertools import unique_everseen

path.append("\\Program Files\\Microsoft.NET\\ADOMD.NET\\150")
from pyadomd import Pyadomd


class PowerBILocal:
    """
    Looks for local PowerBI instances, returns port number, database name, \
    file path and connection strings.
    """

    pbi_connection_data = []
    pbi_connection_string_list = []

    def __str__(self):
        return "Power BI"

    @classmethod
    def power_bi_pid(cls):
        """Returns local PowerBI pid instances."""
        powerbi_pid = os.popen(  # Looks for pid.
            'tasklist | findstr "PBIDesktop.exe"'
        ).readlines()
        if not powerbi_pid:
            raise Exception("No PowerBI local instances.")
        pid_list = [
            re.sub("\\s+", " ", f).split(" ")[1] for f in powerbi_pid
        ]  # Extract pid from string.
        return pid_list

    @classmethod
    def power_bi_file(cls, pid_list):
        """Returns local PowerBI instances file pathes."""
        if not pid_list:
            raise Exception("No Power Bi local instances.")
        pb_files = []
        for pid in pid_list:
            p_name = psutil.Process(int(pid))  # Looks for .pbix filename.
            process_files = p_name.open_files()
            pb_file = [  # Extract .pbix subproceses, not temp files.
                p_file.path
                for p_file in process_files
                if ".pbix" in p_file.path and "TempSaves" not in p_file.path
            ]
            if pb_file:
                pb_files.append(pb_file[0])
            else:  # If .pbix files is unsaved.
                pb_files.append("No saved file.")
        return pb_files

    @classmethod
    def power_bi_port(cls, pid_list):
        """
        Returns PowerBI local instances ports.
        """
        if not pid_list:
            raise Exception("No Power BI local instances.")
        ports = []
        for pid_el in pid_list:  # For every pid...
            port_lines = os.popen(
                'netstat -a -n -o | find "' + pid_el + '"'
            ).readlines()  # Looks for port.

            for port_line in port_lines:  # Extract port number from string.
                if re.sub("\\s+", " ", port_line).split(" ")[3][0:6] == "[::1]:":
                    ports.append(
                        re.sub("\\s+", " ", port_line)
                        .split(" ")[3]
                        .replace("[::1]:", "")
                    )

        ports = list(unique_everseen(ports))
        return ports

    @classmethod
    def power_bi_database(cls, ports_files: list):
        """Returns PowerBI local instances databases"""
        if not ports_files:
            raise Exception("No Power BI local instances.")
        databases = []
        for port in ports_files:  # Gets database_id from tabular model.
            connection_string = (
                r"Provider=MSOLAP;Data Source=localhost:" + str(port) + ";"
            )
            query_string = """select [DATABASE_ID] from $SYSTEM.DBSCHEMA_CATALOGS"""
            with Pyadomd(connection_string) as conn:
                with conn.cursor().execute(query_string) as cur:
                    dataset = cur.fetchall()
                    databases.append(dataset[0][0])
        return list(zip(ports_files, databases))

    @classmethod
    def pbi_connection_string(cls):
        """Wrapper for previous functions"""
        if not PowerBILocal.power_bi_pid():
            raise Exception("No Power BI  local instances.")
        # Merge lists from previous functions.
        cls.pbi_connection_data = list(
            zip(
                PowerBILocal.power_bi_file(PowerBILocal.power_bi_pid()),
                [
                    f[1]
                    for f in PowerBILocal.power_bi_database(
                        PowerBILocal.power_bi_port(PowerBILocal.power_bi_pid())
                    )
                ],
                PowerBILocal.power_bi_port(PowerBILocal.power_bi_pid()),
            )
        )
        # Returns connection strings.
        for conn_data in cls.pbi_connection_data:
            cls.pbi_connection_string_list.append(
                r"Provider=MSOLAP;Data Source=localhost:{0}".format(conn_data[2])
                + ";Catalog="
                + conn_data[1]
                + ";"
            )
        return PowerBILocal()
