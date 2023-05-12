# ETL-excel-to-hyper-tableau
 A personal ETL project of mine that extracts data from Excel, queries it via SQL and Python, and loads the queried data to a .hyper Tableau file.

**How to use this project:**

When testing, file run_main_example.py is the main file. The only requirement for running the program is to have a file location
to specify for the SQL database, and, for the current repository code, to place the provided Excel file in your current working
directory. Feel free to change the query bundles as you please! See below for the QueryBundle format:

QueryBundle format:
export_file_name: The .hyper export file name created in your working directory
query_strings: The queries in SQL format. Any time a sheet name is specified, it must be followed with '.sheet'. This allows
                the program to detect where sheet names are located in the query, so that it can update the sheets when iterating
                through files. See the QueryBundle in run_main_example.py for an example.
matches: A substring or file name of each file in the current working directory, where that file and specified sheets are to be queried for each
            query in the QueryBundle
sheets: An iterable of all sheet names referred to in query_strings - note: the program could possibly be improved by pulling sheet names
        automatically from the queries, as they are denoted by '.sheet'
combine_columns: If True, queried columns are combined vertically, with the first column of each row denoting the query and match of its row 