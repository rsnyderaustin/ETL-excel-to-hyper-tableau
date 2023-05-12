import os
import tkinter

from query_bundle import QueryBundle
from query_iterator import QueryIterator
from query import Query
from tkinter import Tk, filedialog


def create_query_bundles():
    complaints_by_bank = QueryBundle(
        export_file_name="complaints_by_bank",
        query_strings=["SELECT company, company_response_to_consumer, COUNT(company_response_to_consumer) "
                        "FROM Sheet1.sheet "
                        "GROUP BY company, company_response_to_consumer",
                       "SELECT company, COUNT(company) "
                       "FROM Sheet1.sheet "
                       "GROUP BY company"
        ],
        query_names = ['complaint_counts_by_company', 'num_of_complaints_per_company'],
        matches=['consumer_complaints.xlsx', 'consumer_complaints1.xlsx'], # Matches can be any substring of a file in the current directory unique to that file
        sheets=['Sheet1'],
        pivot_table=True
    )

    # Method handles creating its own bundle output,
    # prevents user error in forgetting to append a QueryBundle to the list
    query_bundles = []
    local_values = locals().copy()
    for value in local_values.values():
        if isinstance(value, QueryBundle):
            query_bundles.append(value)
    return query_bundles


def _prompt_file_path():
    root = Tk()
    root.withdraw()

    file_path = filedialog.askdirectory(title="Select a location on your machine to store the SQL database")
    file_path = f"{file_path}/austin_snyder_sql_database.db"
    return file_path


def main():
    query_bundles = create_query_bundles()

    database_path = _prompt_file_path()
    query_iterator = QueryIterator(directory=os.listdir(),
                                   database_path=database_path,
                                   query_bundles=query_bundles)
    query_iterator.process_queries()


main()
