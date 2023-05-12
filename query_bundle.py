from typing import Union, Iterable
from query import Query

class QueryBundle:

    def __init__(self, export_file_name: str,
                 matches: Iterable,
                 combine_columns: bool,
                 query_strings: Union[str, list[str]],
                 query_names: Union[str, list[str]],
                 sheets: Union[str, Iterable]):

        self.matches = matches
        self.sheets = sheets
        self.combine_columns: bool = combine_columns
        self.export_file_name = export_file_name
        self.queried_dfs = {}

        # Cast variables to a list in case they were passed as a string
        if isinstance(query_strings, str):
            self.query_strings = [query_strings]
        else:
            self.query_strings = query_strings

        if isinstance(query_names, str):
            self.query_names = [query_names]
        else:
            self.query_names = query_names

        if isinstance(sheets, str):
            self.sheets = [sheets]
        else:
            self.sheets = sheets

        self.queries = self.separate_query_bundle()

    def separate_query_bundle(self):
        separated_queries = []
        combine_columns = self.combine_columns
        for query_str, query_name in zip(self.query_strings, self.query_names):
            new_query = Query(query_name=query_name, query_str=query_str, combine_columns=combine_columns)
            separated_queries.append(new_query)
        return separated_queries

