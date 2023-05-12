from typing import Union, Iterable
from query import Query

class QueryBundle:

    def __init__(self, export_file_name: str,
                 matches: Iterable,
                 pivot_table: bool,
                 query_strings: Union[str, list[str]],
                 query_names: Union[str, list[str]],
                 sheets: Union[str, Iterable]):
        """

        :param export_file_name: The name of the .hyper export file created in the working directory.
        :param matches: A unique substring or file name of each file in the current working directory, where that file
        and specified sheets are to be queried for each query in the QueryBundle.
        :param pivot_table: If True, columns for each query are combined vertically, with the first column of each
        row denoting the query and match of its row. The primary intended use for this variable is to combine
        a large number of queries with small outputs (ex: The number of insurance claims for each of three groups
        across 20 separate year files.
        :param query_strings: The queries, in SQL, that will iterate over the file matches and Excel sheets in the
        QueryBundle. Any time an Excel sheet name is specified in a query, it must be followed with '.sheet'. This
        allows for parsing of sheet names when iterating over queries.
        :param query_names:
        :param sheets: An iterable of all Excel sheet names referred to in query_strings.
        """

        self.matches = matches
        self.sheets = sheets
        self.pivot_table: bool = pivot_table
        self.export_file_name = export_file_name
        self.queried_dfs_by_query_name = {}

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
        pivot_table = self.pivot_table
        for query_str, query_name in zip(self.query_strings, self.query_names):
            new_query = Query(query_name=query_name, query_str=query_str, pivot_table=pivot_table)
            separated_queries.append(new_query)
        return separated_queries

