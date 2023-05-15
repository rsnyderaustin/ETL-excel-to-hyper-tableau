import os
import tkinter

from query_bundle import QueryBundle
from query_iterator import QueryIterator
from query import Query
from tkinter import Tk, filedialog


def create_query_bundles():
    complaints_by_bank = QueryBundle(
        export_file_name="complaints_by_bank",
        query_strings=["SELECT company, product, "
                       "COUNT(product) as number_of_complaints "
                       "FROM Sheet1.sheet "
                       "WHERE company='Bank of America'"
                       "GROUP BY product "
                       "HAVING COUNT(company_response_to_consumer)>10",
                       "SELECT company, COUNT(company) "
                       "FROM Sheet1.sheet "
                       "GROUP BY company "
        ],
        query_names = ['complaint_counts_by_company', 'num_of_complaints_per_company'],
        matches=['consumer_complaints.xlsx', 'consumer_complaints1.xlsx'], # Matches can be any substring of a file in the current directory unique to that file
        sheets=['Sheet1'],
        pivot_table={'complaint_counts_by_company': True, 'num_of_complaints_per_company': False}
    )

    # Method handles creating its own bundle output,
    # prevents user error in forgetting to append a QueryBundle to the list
    query_bundles = []
    local_values = locals().copy()
    for value in local_values.values():
        if isinstance(value, QueryBundle):
            query_bundles.append(value)
    return query_bundles


def _prompt_export_file_path():
    root = Tk()
    root.withdraw()

    file_path = filedialog.askdirectory(title="Select a location on your machine to store the SQL database")
    file_path = f"{file_path}/austin_snyder_sql_database.db"
    return file_path


def main():
    query_bundles = create_query_bundles()

    # If you would like to use a different file or set of files for querying, please change the directory assignment
    # here to that file name. The file should be placed into the ETL-excel-to-hyper-tableau folder on your machine.
    # Remember that you will have to change the query as well to refer to your table and its specific data.
    database_path = _prompt_export_file_path()
    query_iterator = QueryIterator(directory=['consumer_complaints.xlsx', 'consumer_complaints1.xlsx'],
                                   database_path=database_path,
                                   query_bundles=query_bundles)
    query_iterator.process_queries()


main()
