import psycopg2
import csv
import io
import os
import json
from datetime import datetime

def load_configuration():
    """Load the configuration from the config.json file."""
    with open("config.json") as config_file:
        config = json.load(config_file)
    return config

def get_database_connection():
    """Create a connection to the PostgreSQL database."""
    config = load_configuration()
    conn = psycopg2.connect(**config["database"])
    conn.set_session(autocommit=True)
    return conn

def create_table_if_not_exists(conn, table_name, create_query):
    """Create a table if it does not exist in the database."""
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {create_query}")

def load_csv_data(conn, table_name, csv_file):
    """Load CSV data into a PostgreSQL table."""
    with conn.cursor() as cursor:
        with open(csv_file, "r") as file:
            next(file)  # Skip the header row
            cursor.execute("SET datestyle = 'ISO, MDY'")
            cursor.copy_from(file, table_name, sep=",")

def load_csv_data_copy (conn, csv_file, table_name):
    """Load CSV data into a PostgreSQL table using COPY function."""
    with open(csv_file, "r") as file:
        file_content = file.read()
        file_obj = io.StringIO(file_content)
        with conn.cursor() as cursor:
                cursor.execute("SET datestyle = 'ISO, MDY'")
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE, QUOTE '\"', ESCAPE '\"')",
                    file_obj
                )

def insert_into_control_table(conn, filename, tablename):
    """Insert a record into the control table."""
    with conn.cursor() as cursor:
        timestamp = datetime.now()
        cursor.execute("INSERT INTO controlfilesloaded (filename, tablename, timeloaded) VALUES (%s, %s, %s)",
                       (filename, tablename, timestamp))

def get_filenames_from_path(path, conn):
    """Get a list of all new csv files, files that don't match with control table."""
    path_files = []
    with conn.cursor() as cursor:
        cursor.execute("SELECT filename FROM controlfilesloaded")
        loaded_files = [row[0].lower() for row in cursor.fetchall()]

    for filename in os.listdir(path):
        if filename.endswith('.csv') and filename.lower() not in loaded_files:
            path_files.append(filename.lower())
    return path_files


def main():
    config = load_configuration()
    path = config["file_path"]
    loading_order = config["loading_order"]
    conn = get_database_connection()
    create_table_if_not_exists(conn, "controlfilesloaded", config["tables"]["controlfilesloaded"])
    
    try:
        with conn:
            for table in loading_order:
                filenames = get_filenames_from_path(path, conn)
                for filename in filenames:
                    if table in filename:
                        table_name = filename.lower().replace('.csv', '').rsplit('_',1)[0]
                        create_table_query = config["tables"][table_name]
                        create_table_if_not_exists(conn, table_name, create_table_query)
                        print(f"Starting to read {table} files!")
                        csv_file = os.path.join(path, filename)

                        if table_name in ["territories", "product_subcategories"]:
                            load_csv_data(conn, table_name, csv_file)
                        elif table_name in ["products", "customers", "sales"]:
                            load_csv_data_copy(conn, csv_file, table_name)
                        else:
                            print("Table not identified")
                        
                        insert_into_control_table(conn, filename, table_name)
                        print(f"{table} table loaded successfully!")
    except psycopg2.OperationalError as e:
        print("Could not start a connection to the database")
        print(e)
    except psycopg2.DatabaseError as e:
        print("An error occurred while working with the database, Rolling back!")
        print(e)
        conn.rollback()

if __name__ == "__main__":
    main()
