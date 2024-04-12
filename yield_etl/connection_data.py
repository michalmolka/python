"""Creates connection data"""
import pyodbc


class ConnectionData:
    """Creates connection data, returns cursor."""

    def __str__(self):
        return "Connection Data Class"

    def __init__(self, s_server, s_database, d_server, d_database):
        self.s_server = s_server
        self.s_database = s_database
        self.d_server = d_server
        self.d_database = d_database
        self.s_connection_string = (
            "Driver={ODBC Driver 17 for SQL Server};Server="
            + self.s_server
            + ";Database="
            + self.s_database
            + ";Trusted_Connection=yes;"
        )
        self.d_connection_string = (
            "Driver={ODBC Driver 17 for SQL Server};Server="
            + self.d_server
            + ";Database="
            + self.d_database
            + ";Trusted_Connection=yes;"
        )
        self.s_cur = ""
        self.d_cur = ""

    def create_connection(self):
        """Creates connection cursor."""
        s_conn = pyodbc.connect(self.s_connection_string)
        d_conn = pyodbc.connect(self.d_connection_string)
        self.s_cur = s_conn.cursor()
        self.d_cur = d_conn.cursor()

        return ConnectionData(
            self.s_server, self.s_database, self.d_server, self.d_database
        )
