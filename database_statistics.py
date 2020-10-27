"""Retrievs, transform data from DMV views, returns tables with statistics."""

from sys import path
import pandas as pd

path.append("\\Program Files\\Microsoft.NET\\ADOMD.NET\\150")
from pyadomd import Pyadomd


class DatabasesStatistics:
    """Gets tables statistics"""

    def __init__(self, connection_info, position, mbyte):
        self.connection_info = connection_info
        self.numeric_columns = ["Data_Size", "Dictionary_Size", "Hierarchy_Size", "Sum"]
        self.connection_string = ""
        # Checks if instance concerns SQL Server or PowerBI
        if self.connection_info.__class__.__name__ == "SQLServer":
            self.connection_string = self.connection_info.sql_server_connection_string_list[
                position
            ]
        elif self.connection_info.__class__.__name__ == "PowerBILocal":
            self.connection_string = self.connection_info.pbi_connection_string_list[
                position
            ]

        self.dataset_data = pd.DataFrame()
        self.dataset_dictionary = pd.DataFrame()
        self.dataset_hierarchy = pd.DataFrame()
        self.dataset_cardinality = pd.DataFrame()
        self.datasets = []
        self.byte_mbyte = ""
        if mbyte == 1:  # Set megabytes view or stardard bytes.
            self.byte_mbyte = 1000000
        else:
            self.byte_mbyte = 1

    def __str__(self):
        return "Database statistics"

    def get_statistics(self):
        """Gets tables statistics"""
        # Fixed queries to get data from DMV.
        query_string = []
        query_string.append(
            """select [DIMENSION_NAME] AS [Table_Name], [COLUMN_ID] as \
            [Column_Name], [USED_SIZE]*1.0 AS [Data_Size] from \
            $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS WHERE \
            RIGHT (LEFT(TABLE_ID, 2),1) <> '$'"""
        )
        query_string.append(
            """select [DIMENSION_NAME] AS [Table_Name],  [COLUMN_ID] \
            as [Column_Name], [DICTIONARY_SIZE]*1.0 AS [Dictionary_Size] from \
            $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS where COLUMN_TYPE = 'BASIC_DATA'"""
        )
        query_string.append(
            """select  [DIMENSION_NAME] AS [Table_Name], [TABLE_ID] \
            as [Column_Name], [USED_SIZE]*1.0 AS [Hierarchy_Size] from \
            $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMN_SEGMENTS WHERE LEFT(TABLE_ID, 2) = 'H$'"""
        )
        query_string.append(
            """select  [DIMENSION_NAME] AS [Table_Name], [TABLE_ID] \
            as [Column_Name], [ROWS_COUNT]*1.0-3 AS [Cardinality_Size] \
            from $SYSTEM.DISCOVER_STORAGE_TABLES WHERE LEFT(TABLE_ID, 2) = 'H$'"""
        )
        for query in query_string:
            with Pyadomd(self.connection_string) as conn:
                with conn.cursor().execute(query) as cur:
                    self.datasets.append(
                        pd.DataFrame(
                            cur.fetchone(), columns=[i.name for i in cur.description]
                        )
                    )

    def transform_data(self):
        """Transforms retrieved data"""
        # Change Column_Name to standard format: "column name (number)"
        for dataset_variable in [self.datasets[3], self.datasets[2]]:
            dataset_variable.Column_Name = dataset_variable.Column_Name.str.split(
                "$"
            ).str[2]

        for i, dataset in enumerate(self.datasets):
            self.datasets[i] = (
                dataset.groupby(["Table_Name", "Column_Name"])[dataset.columns[2]]
                .sum()
                .reset_index()
            )
        # Join all datasets into one.
        merged = pd.merge(
            pd.merge(
                self.datasets[3],
                self.datasets[0],
                on=["Table_Name", "Column_Name"],
                how="outer",
            ),
            pd.merge(
                self.datasets[1],
                self.datasets[2],
                on=["Table_Name", "Column_Name"],
                how="outer",
            ),
            on=["Table_Name", "Column_Name"],
            how="outer",
        )

        # Generate subtotals for tables.
        sub_total = (
            merged.groupby(["Table_Name"])[
                ["Data_Size", "Dictionary_Size", "Hierarchy_Size"]
            ]
            .sum()
            .reset_index()
        )

        sub_total.insert(1, "Column_Name", "SubTotal")
        sub_total.insert(2, "Cardinality_Size", None)
        # Generate total for entire dataset.
        total = (
            pd.DataFrame(sub_total.sum())
            .transpose()
            .assign(Table_Name="Total", Column_Name="Total", Cardinality_Size=None)
        )
        # Add total, subtotal rows and Sum column.
        final_dataframe = merged.append(sub_total).append(total)
        final_dataframe["Sum"] = (
            final_dataframe["Data_Size"]
            + final_dataframe["Dictionary_Size"]
            + final_dataframe["Hierarchy_Size"]
        )
        # Bytes or Mbytes data view, and round to 2 digits.
        final_dataframe[self.numeric_columns] = final_dataframe[
            self.numeric_columns
        ].applymap(lambda x: round(x / self.byte_mbyte, 2))
        # Add thousand separator.
        final_dataframe[self.numeric_columns] = final_dataframe[
            self.numeric_columns
        ].applymap("{:,}".format)

        return final_dataframe
