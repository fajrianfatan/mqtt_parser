import pandas as pd

# Replace 'input.csv' with the path to your CSV file
csv_file = 'messages_converted_single.csv'

# Replace 'output.xlsx' with the desired output Excel file name
excel_file = 'messages_csv_to_table.xlsx'

# Read the CSV file into a pandas DataFrame
data_frame = pd.read_csv(csv_file)

# Convert DataFrame to an Excel file
data_frame.to_excel(excel_file, index=False)
