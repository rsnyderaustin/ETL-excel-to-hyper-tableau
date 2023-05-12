import os
import re


class Query:

    def __init__(self, query_name: str, query_str: str, combine_columns: bool):
        self.query_str = query_str
        self.combine_columns = combine_columns
        self.query_name = query_name

    # Replaces each "sheet_name.sheet" in the query to include the properly formatted file name
    def format_query(self, file_name: str):
        base_file, extension = os.path.splitext(file_name)
        # SQL queries cannot contain a file extension or will error
        file_name = base_file
        query_parts = re.split(" ", self.query_str)
        slice_identifier = ".sheet"
        for i, part in enumerate(query_parts):
            index = part.rfind(slice_identifier)
            if index != -1:
                query = part[:index]
                query_parts[i] = f"{file_name}_{query}_sheet"
        formatted_query = ''.join(
            [' ' + x if x not in [' ', ''] and query_parts[i - 1] not in [' ', ''] else x for i, x in
             enumerate(query_parts)]).strip()
        return formatted_query