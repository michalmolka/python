"""Main script"""

import connection_data as cc
import data_flow as df

# Connect to databases, creates cursors.
c_data = cc.ConnectionData(
    r"localhost\SQLSERVER2019",
    "StackOverflow",
    r"localhost\SQLSERVER2019",
    "StackOverflow",
)
c_data.create_connection()

flow = df.DataFlow(
    c_data,
    "dbo.Badges_Source",
    "dbo.Badges_Destination",
    "[Id], [Name],[UserId],[Date]",
    8473372,
)
flow.read_write_data()
