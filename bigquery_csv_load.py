import csv
import uuid
import datetime
from google.cloud import bigquery

# Set up BigQuery client
client = bigquery.Client()

# Define the BigQuery dataset and table information
dataset_id = 'lgwm'
table_id = 'mycodebq'

# Define the path to the CSV file
csv_file_path = 'bigquery.csv'

# Read the CSV file and add GUID and insertion date columns
rows = []
with open(csv_file_path, 'r') as file:
    reader = csv.reader(file)
    header = next(reader)  # Get the header row
    header.extend(['guid', 'audit_insertion_date'])  # Add 'guid' and 'insertion_date' columns to the header

    for row in reader:
        row.append(str(uuid.uuid4()))  # Generate and add a new GUID to the row
        row.append(str(datetime.datetime.now()))  # Add current insertion date to the row
        rows.append(row)

# Create the BigQuery table schema
schema = [
    bigquery.SchemaField('firstname', 'STRING'),
    bigquery.SchemaField('department', 'INTEGER'),
    bigquery.SchemaField('startdate', 'DATE'),
    bigquery.SchemaField('guid', 'STRING'),
    bigquery.SchemaField('audit_insertion_date', 'TIMESTAMP'),
]

# Create BigQuery table if it doesn't exist
table_ref = client.dataset(dataset_id).table(table_id)
try:
    client.get_table(table_ref)
except:
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

# Insert data into BigQuery table
table = client.get_table(table_ref)  # Retrieve the table object
errors = client.insert_rows(table, rows)  # Insert the rows into the table

if errors == []:
    print('Data inserted successfully.')
else:
    print('Encountered errors while inserting data: {}'.format(errors))
