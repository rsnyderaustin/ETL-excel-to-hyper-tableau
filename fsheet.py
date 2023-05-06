from typing import Any

import pandas as pd
import os


# Represents one file and one sheet pairing
class Fsheet:
    sheets: list[str] | Any

    def __init__(self, file_name, sheet):
        self.file_name = file_name
        self.sheet = sheet

        self._dataframe = None
        self.sql_table_name = None

    @property
    def _dataframe(self):
        if self._dataframe is not None:
            return self._dataframe
        else:
            raise Exception(f"Requested dataframe from Fsheet {(self.file_name, self.sheet)} does not exist")

    @property
    def sql_table_name(self):
        return self.sql_table_name

    @_dataframe.setter
    def _dataframe(self, value):
        self.__dataframe = value

    def create_dataframe(self):
        df = pd.read_excel(io=self.file_name, sheet_name=self.sheet)
        self._dataframe = df
        return df

    def create_sql_table_name(self):
        base_file, extension = os.path.splitext(self.file_name)
        sql_table_name = f"{base_file}_{self.sheet}_sheet"
        self.sql_table_name = sql_table_name
        return sql_table_name

    @sql_table_name.setter
    def sql_table_name(self, value):
        self._sql_table_name = value

