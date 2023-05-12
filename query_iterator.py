import itertools
import os
from collections import namedtuple

import pandas as pd
from query_bundle import QueryBundle
from typing import Optional
import sqlite3
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, \
    SqlType, TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName
from fsheet import Fsheet


class QueryIterator:
    def __init__(self, directory: list, database_path: str, query_bundles: list[QueryBundle]):
        """
        :param directory: The list of files in the current working directory to be queried.
        :param database_path: The file path where the SQL .db file will be created.
        :param query_bundles: The QueryBundle objects that will be iterated over by the QueryIterator.
        """
        self.directory = directory

        self.database_path = database_path

        # Distinct (file, sheet) pair objects
        self.fsheets = []
        self._map_match_to_file = {}

        self.query_bundles = query_bundles

    def process_queries(self):

        # Pairs each match to its associated file name
        self._match_directory_files()

        self._generate_distinct_fsheets()

        self._fsheet_df_to_sql_database()

        # Fills queried dfs dict(export_file_name: dict(query_name: (import_file_name, DataFrame))
        # Each QueryBundle has a unique export file name
        self._query_dataframes()

        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "hyperfile.log") as hyper:
            self._export_to_hyper(hyper)

    # Pairs matches with appropriate files, match:file_name
    def _match_directory_files(self):
        # Helper
        def filter_directory(directory) -> list:
            filtered_directory = []
            for file_name in directory:
                base, extension = os.path.splitext(file_name)
                if extension in (".xlsx", ".xls"):
                    filtered_directory.append(file_name)
            if len(filtered_directory) == 0:
                raise Exception("No excel files found in the current directory")
            return filtered_directory

        # Helper
        def find_directory_match(directory, match) -> str:
            for file_name in directory:
                if str(match) in file_name:
                    return file_name
            # Error if no match is found
            raise Exception(f"{match} has no associated file name in the current directory")

        # Main function begins here
        filtered_directory = filter_directory(self.directory)
        map_match_to_file = {}
        for query_bundle in self.query_bundles:
            for match in query_bundle.matches:
                # Don't pair previously paired matches
                if match not in map_match_to_file:
                    file_match = find_directory_match(filtered_directory, match)
                    self._map_match_to_file[match] = file_match

    def _generate_distinct_fsheets(self):
        # {file_name: list[sheet]}
        processed_file_sheet_pairs = {str: list}
        for query_bundle in self.query_bundles:
            for match, sheet in itertools.product(query_bundle.matches, query_bundle.sheets):
                file = self._map_match_to_file[match]
                if file not in processed_file_sheet_pairs:
                    self.fsheets.append(Fsheet(file_name=file, sheet=sheet))
                    processed_file_sheet_pairs[file] = [sheet]
                elif sheet not in processed_file_sheet_pairs[file]:
                    self.fsheets.append(Fsheet(file, sheet))
                    processed_file_sheet_pairs[file].append(sheet)

    def _fsheet_df_to_sql_database(self):
        sql_connection = sqlite3.connect(self.database_path)
        for fsheet in self.fsheets:
            df = fsheet.create_dataframe()
            sql_table_name = fsheet.create_sql_table_name()
            df.to_sql(name=sql_table_name, con=sql_connection, if_exists='replace', index=False)
        sql_connection.close()

    def _query_dataframes(self):
        # Required so that column names pulled from different files in the data are specific to their associated file
        def format_column_names(df, match) -> list:
            columns = df.columns
            new_column_names = []
            for column_name in columns:
                new_name = f"{match}_{column_name}"
                new_column_names.append(new_name)
            rename_dict = dict(zip(columns, new_column_names))
            renamed_df = df.rename(rename_dict, axis='columns')
            return renamed_df

        sql_connection = sqlite3.connect(self.database_path)
        File_DataFrame_Tuple = namedtuple('File_DataFrame_Tuple', ['file_name', 'queried_df'])
        for query_bundle in self.query_bundles:
            for query in query_bundle.queries:
                # Initialize the queried_df value to have tuple (import_file_name, DataFrame) stored in following code
                query_bundle.queried_dfs_by_query_name[query.query_name] = []

                for match in query_bundle.matches:
                    import_file_name = self._map_match_to_file[match]
                    formatted_query_str = query.format_query(import_file_name)
                    queried_df = pd.read_sql_query(formatted_query_str, sql_connection)
                    # Format DataFrame columns to distinct column names if columns are not being combined later
                    if not query.pivot_table:
                        queried_df = format_column_names(queried_df, match)

                    # dict(query_name: (match, DataFrame))
                    file_dataframe_tuple = File_DataFrame_Tuple(file_name=import_file_name, queried_df=queried_df)
                    query_bundle.queried_dfs_by_query_name[query.query_name].append(file_dataframe_tuple)
        sql_connection.close()

    def _export_to_hyper(self, hyper: HyperProcess):
        # Column 0 denotes each row's query-specific match
        def _pivot_df(file_dataframe_tuples) -> pd.DataFrame:
            sample_df = file_dataframe_tuples[0].queried_df
            pivoted_columns = sample_df.index.tolis()
            



            """# {column_name: [column_data]}
            new_columns = {}

            index_col_name = 'query_file_index'
            new_columns[index_col_name] = []

            sample_tuple = file_dataframe_tuples[0]
            sample_df = sample_tuple.queried_df
            for column in sample_df.columns:
                new_columns[column] = []
            for tuple in file_dataframe_tuples:
                new_columns[index_col_name].append(str(tuple.file_name))

            # Establish column names
            sample_df = file_dataframe_tuples[0].queried_df
            for column in sample_df.columns.tolist():
                new_columns[column] = []

            # Fill dictionary with data
            dataframes = [tuple.queried_df for tuple in file_dataframe_tuples]
            for df in dataframes:
                for col_name, col_values in df.items():
                    new_columns[col_name].extend(col_values)
            return pd.DataFrame(new_columns)"""

        """# Returns a list of rows, with each row representing an index of the aggregate of method DataFrames
        def _aggregate_dataframes(self, dataframes: list[pd.DataFrame]) -> list[list]:
            max_df_length = 0
            df_col_length: dict[str, int] = {}
    
            df_aggregate_rows = []
    
            # Finds the max length of all dataframes, and fills df_col_length dict for each column
            for df in dataframes:
                if len(df) > max_df_length:
                    max_df_length = len(df)
                for col in df.columns:
                    df_col_length[col] = len(df[col])
    
            # 'i' represents the row index that is currently being processed
            for i in range(max_df_length):
                df_aggregate_rows.append([])
                for df in dataframes:
                    for col in df.columns:
                        column_data = df[col]
                        column_length = len(column_data)
                        if i < column_length:
                            df_aggregate_rows[i].append(column_data[i])
                        else:
                            df_aggregate_rows[i].append(None)
            return df_aggregate_rows"""

        def cleanup_SQL_tables(database_path):
            sql_connnection = sqlite3.connect(database_path)
            cursor = sql_connnection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table[0]};")
            sql_connnection.commit()
            sql_connnection.close()

        for query_bundle in self.query_bundles:
            hyper_table_name = f"{query_bundle.export_file_name}.hyper"
            with Connection(hyper.endpoint, hyper_table_name, CreateMode.CREATE_AND_REPLACE) as connection:
                for query in query_bundle.queries:
                    file_dataframe_tuples = query_bundle.queried_dfs_by_query_name[query.query_name]
                    if query.pivot_table:
                        df = _pivot_df(file_dataframe_tuples)
                    else:
                        list_of_dataframes = [tuple.queried_df for tuple in file_dataframe_tuples]
                        df = pd.concat(list_of_dataframes, axis=1)
                    row_data = df.values.tolist()

                    df_hyper_columns = self._create_df_column_objects(df)

                    table_def = TableDefinition(query.query_name, df_hyper_columns)

                    connection.catalog.create_table(table_def)

                    with Inserter(connection, table_def) as inserter:
                        for row in row_data:
                            inserter.add_row(row)
                        inserter.execute()

                    cleanup_SQL_tables(self.database_path)

    @staticmethod
    # Creates HyperAPI.Column objects
    def _create_df_column_objects(df: pd.DataFrame) -> list[TableDefinition.Column]:
        def df_columns_sqltypes(df) -> dict[str: SqlType]:
            dtype_to_sqltype_map = {
                "int64": SqlType.int(),
                "int": SqlType.int(),
                "float": SqlType.double(),
                "float64": SqlType.double(),
                "datetime": SqlType.timestamp(),
                "string": SqlType.varchar(50),
                "object": SqlType.varchar(50),
                "'O'": SqlType.varchar(50)

            }

            dtypes = df.dtypes.to_dict()
            sql_types = {}
            for column_name in dtypes:
                dtype = dtypes[column_name].name
                sql_type = dtype_to_sqltype_map[dtype]
                sql_types[column_name] = sql_type
            return sql_types

        table_def_columns = []
        # {column_name: SqlType}
        columns_and_sql_types = df_columns_sqltypes(df)
        for col in columns_and_sql_types:
            sql_type = columns_and_sql_types[col]

            new_table_def_column = TableDefinition.Column(name=col, type=sql_type)
            table_def_columns.append(new_table_def_column)

        return table_def_columns

