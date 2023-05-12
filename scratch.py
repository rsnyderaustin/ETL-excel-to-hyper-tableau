import sqlite3

import pandas as pd

def export_csv_to_excel(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Take the first 1000 rows
    df = df.head(1000)

    # Get the base name of the CSV file
    base_name = csv_file.rsplit('.', 1)[0]

    # Export to Excel
    excel_file = f'{base_name}.xlsx'
    df.to_excel(excel_file, index=False)
    print(f'Exported to {excel_file}')

# Specify the path to your CSV file
csv_file = 'C:/Users/austisnyder/Downloads/Data _Analysis/PyCharmWorkingDirectory/consumer_complaints.csv'

# Call the function to export CSV to Excel
export_csv_to_excel(csv_file)
