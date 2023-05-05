import os
import csv
from sqlalchemy import create_engine, text

# Function to convert Spark SQL to BigQuery SQL
def convert_spark_to_bigquery(spark_sql_query):
    # Create a Spark SQL engine
    spark_engine = create_engine("spark://")

    # Parse the Spark SQL query
    parsed_query = spark_engine.parse_sql(spark_sql_query)

    # Create a BigQuery engine
    bigquery_engine = create_engine("bigquery://")

    # Compile the parsed query into BigQuery SQL
    bigquery_sql_query = text(str(parsed_query.compile(bigquery_engine)))

    return bigquery_sql_query

root_directory = '/home/outsider_analytics/Code/spellbook/models/balancer'

# Walk through the root directory and its subdirectories
for root, dirs, files in os.walk(root_directory):
    for file in files:
        if file.endswith('.sql'):
            file_path = os.path.join(root, file)
            
            # Read the SQL query from the file
            with open(file_path, 'r') as sql_file:
                spark_sql_query = sql_file.read()

            # Convert the query
            try:
                bigquery_sql_query = convert_spark_to_bigquery(spark_sql_query)
                print(f"Converted query for {file_path}:\n{bigquery_sql_query}\n")
            except Exception as e:
                print(f"Error converting query for {file_path}: {e}\n")
