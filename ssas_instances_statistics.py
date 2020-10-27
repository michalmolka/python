"""Main script"""

import sql_server as sqls
import power_bi_local as pb
import database_statistics as ts

# PowerBI local, active files.
pbi = pb.PowerBILocal()
pbi.pbi_connection_string()

for i, abc in enumerate(pbi.pbi_connection_string_list):
    print(i, ":", abc)
for i, abc in enumerate(pbi.pbi_connection_data):
    print(i, ":", abc)

pbi_tabular_statitics = ts.DatabasesStatistics(pbi, 1, 0)
pbi_tabular_statitics.get_statistics()
print(pbi_tabular_statitics.transform_data())

# SQL Server SSAS local instances.
sql_server = sqls.SQLServer()
sql_server.sql_server_databases(
    sql_server.sql_server_instances().sql_server_instances_list
)
sql_server.sql_server_connection_string()

for i, abc in enumerate(sql_server.sql_server_connection_string_list):
    print(i, ":", abc)
for i, abc in enumerate(sql_server.instance_database_list):
    print(i, ":", abc)

sql_server_tabular_statitics = ts.DatabasesStatistics(sql_server, 1, 0)
sql_server_tabular_statitics.get_statistics()
print(sql_server_tabular_statitics.transform_data())
