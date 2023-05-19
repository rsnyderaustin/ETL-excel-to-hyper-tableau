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
from openpyxl import Workbook


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

        for query_bundle in self.query_bundles:
            if query_bundle.file_extension in '.hyper':
                with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, "hyperfile.log") as hyper:
                    self._export_to_hyper(query_bundle, hyper)
            elif query_bundle.file_extension in ['.xlsx', '.xls']:
                file_name = f"{query_bundle.export_file_name}{query_bundle.file_extension}"
                if os.path.exists(file_name):
                    os.remove(file_name)
                workbook = Workbook()
                workbook.save(filename=file_name)
                self._export_to_excel(query_bundle, workbook)

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
                raise Exception("No excel files found in the current working directory.")
            return filtered_directory

        # Helper
        def find_directory_match(directory, match) -> str:
            for file_name in directory:
                if str(match) in file_name:
                    return file_name
            # Error if no match is found
            raise Exception(f"Specified match {match} has no associated file name in the current directory")

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

    def _pivot_df(self, file_dataframe_tuples) -> pd.DataFrame:
        # Column names of df are the same column names as the original dataframes
        sample_df = file_dataframe_tuples[0].queried_df
        aggregate_df_dict = {}
        aggregate_df_dict['index'] = []
        aggregate_df_dict.update({col: [] for col in sample_df.columns.tolist()})

        for tuple in file_dataframe_tuples:
            # Add the current file name to the index for the number of rows in that queried dataframe
            base, extension = os.path.splitext(tuple.file_name)
            aggregate_df_dict['index'].extend([base] * len(tuple.queried_df))

            # Add the values in the current tuple dataframe to the aggregate df
            for col in tuple.queried_df.columns:
                aggregate_df_dict[col].extend(tuple.queried_df[col])

        aggregate_df = pd.DataFrame(aggregate_df_dict)
        return aggregate_df

    def _cleanup_SQL_tables(self, database_path):
        sql_connnection = sqlite3.connect(database_path)
        cursor = sql_connnection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table[0]};")
        sql_connnection.commit()
        sql_connnection.close()

    def _export_to_hyper(self, query_bundle: QueryBundle, hyper: HyperProcess):
        # Column 0 denotes each row's query-specific match

        with Connection(hyper.endpoint, query_bundle.export_file_name, CreateMode.CREATE_AND_REPLACE) as connection:
            for query in query_bundle.queries:
                # Each query uses all dataframes in the QueryBundle
                dataframes = query_bundle.queried_dfs_by_query_name[query.query_name]
                if query.pivot_table:
                    df = self._pivot_df(dataframes)
                else:
                    list_of_dataframes = [tuple.queried_df for tuple in dataframes]
                    df = pd.concat(list_of_dataframes, axis=1)
                row_data = df.values.tolist()

                df_hyper_columns = self._create_df_column_objects(df)

                table_def = TableDefinition(query.query_name, df_hyper_columns)

                connection.catalog.create_table(table_def)

                with Inserter(connection, table_def) as inserter:
                    for row in row_data:
                        inserter.add_row(row)
                    inserter.execute()

                self.cleanup_SQL_tables(self.database_path)

    def _export_to_excel(self, query_bundle:QueryBundle, workbook: Workbook):
        file_name = f"{query_bundle.export_file_name}{query_bundle.file_extension}"
        writer = pd.ExcelWriter(file_name, engine='openpyxl')

        for query in query_bundle.queries:
            # Each query uses all dataframes in the QueryBundle
            dataframes = query_bundle.queried_dfs_by_query_name[query.query_name]
            if query.pivot_table:
                df = self._pivot_df(dataframes)
            else:
                list_of_dataframes = [tuple.queried_df for tuple in dataframes]
                df = pd.concat(list_of_dataframes, axis=1)

            df.to_excel(writer, sheet_name=query.query_name, index=False)
        writer._save()
        writer.close()
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
                "string": SqlType.varchar(1000),
                "object": SqlType.varchar(1000),
                "'O'": SqlType.varchar(1000)

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
        col_sql_types = df_columns_sqltypes(df)
        for col in col_sql_types:
            sql_type = col_sql_types[col]

            new_table_def_column = TableDefinition.Column(name=col, type=sql_type)
            table_def_columns.append(new_table_def_column)

        return table_def_columns

