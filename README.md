# ETL-excel-to-hyper-tableau
 A personal ETL project of mine that iterates over Excel files and sheets, transforms the data into DataFrames,
 queries it via SQL and Python, and loads the queried data to a .hyper Tableau file.

**How to use this project:**

When testing, file run_main_example.py is the main file. The only requirements are to have a file location
to specify for the SQL database. The QueryBundle iterates over as many matches, sheets, and queries as you specify in 
the constructor and have included in the working directory. See QueryBundle constructor documentation for details.
If changing files to run on, the directory in run_main_example.py must be changed to either the directory path where the
Excel file(s) are located, or to an iterable of the specific Excel files in the current working directory
