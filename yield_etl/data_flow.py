"""Copy data from one table to another, no SCD1/SCD2"""


class DataFlow:
    """Copy data from source to destination table."""

    def __str__(self):
        return "Data Flow Class"

    def __init__(
        self,
        connection_data: object,
        s_table: str,
        d_table: str,
        columns: str,
        batch_size: int,
    ):
        self.connection_data = connection_data
        self.s_table = s_table
        self.d_table = d_table
        self.columns = columns
        self.batch_size = batch_size
        self.s_query = ""
        self.d_query = (
            "INSERT INTO "
            + self.d_table
            + " ("
            + self.columns
            + ") VALUES ("
            + ("?," * (self.columns.count(",") + 1))[:-1]
            + ")"
        )
        self.result = ""

    def read_write_data(self):
        """Reads data from source table."""
        count_query = (
            "SELECT count(*) FROM "
            + self.s_table
            + " WHERE [Date] between '2018-01-01' and '2019-12-01'"
        )
        count_data = list(self.connection_data.s_cur.execute(count_query))[0][0]

        def range_slicer():
            for i in range(0, count_data, self.batch_size):
                self.s_query = (
                    "SELECT "
                    + self.columns
                    + " FROM "
                    + self.s_table
                    + " where [Date] between '2018-01-01' and '2019-12-01'"
                    + "ORDER BY [Id] OFFSET "
                    + str(i)
                    + " ROWS FETCH NEXT "
                    + str(self.batch_size)
                    + " ROWS ONLY"
                )
                self.connection_data.s_cur.execute(self.s_query)
                self.result = self.connection_data.s_cur.fetchall()
                yield self.result

        self.connection_data.d_cur.fast_executemany = True
        for i, s_el in enumerate(range_slicer()):
            self.connection_data.d_cur.executemany(
                self.d_query, s_el,
            )
            print("Batch: ", i)
        self.connection_data.d_cur.commit()
        return DataFlow(
            self.connection_data,
            self.s_table,
            self.d_table,
            self.columns,
            self.batch_size,
        )

    def write_data(self):
        """Writes data to destination table"""
        self.connection_data.d_cur.fast_executemany = True

        def list_slicer(result_list):
            for i in range(0, len(self.result), self.batch_size):
                yield result_list[i : i + self.batch_size]

        for s_el in list_slicer(self.result):
            self.connection_data.d_cur.executemany(
                self.d_query, s_el,
            )
        self.connection_data.d_cur.commit()
        return "Data has been written."
