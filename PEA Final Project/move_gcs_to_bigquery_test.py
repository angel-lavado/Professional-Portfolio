#!/usr/bin/env python
# coding: utf-8

def move_gcs_to_bigquery(bucket_name, directory_path, table_name, project_id, dataset_id):
    from google.cloud import storage, bigquery

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # List all Parquet files in the given directory
    blobs = bucket.list_blobs(prefix=directory_path)
    parquet_files = [blob for blob in blobs if blob.name.endswith('.parquet')]

    if not parquet_files:
        print(f"No Parquet files found in the directory: {directory_path}")
        return
    
    bq_client = bigquery.Client(project=project_id)
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)


     # Drop the table if it already exists
    try:
        bq_client.delete_table(table_ref)
        print(f'Table {table_name} deleted successfully')
    except:
        print(f'Table {table_name} does not exist, proceeding to create it...')
        
    print(f'Table {table_name} is starting to create...')
    schema = [
    bigquery.SchemaField('id_request', 'INTEGER'),
    bigquery.SchemaField('office_type', 'STRING'),
    bigquery.SchemaField('iata_code_origin', 'STRING'),
    bigquery.SchemaField('iata_code_destination', 'STRING'),
    bigquery.SchemaField('airport_name_origin', 'STRING'),
    bigquery.SchemaField('airport_name_destination', 'STRING'),
    bigquery.SchemaField('seats', 'INTEGER'),
    bigquery.SchemaField('booking', 'STRING'),
    bigquery.SchemaField('request_status', 'STRING'),
    bigquery.SchemaField('request_date', 'TIMESTAMP'),
    bigquery.SchemaField('id_request_movement', 'INTEGER'),
    bigquery.SchemaField('movement_date', 'TIMESTAMP'),
    bigquery.SchemaField('movement', 'STRING'),
    bigquery.SchemaField('comments', 'STRING'),
    bigquery.SchemaField('movement_error_flag', 'BOOLEAN'),
    ]

    table = bigquery.Table(table_ref, schema=schema)
    bq_client.create_table(table)
    print(f'Table {table_name} created successfully')

    for blob in parquet_files:
        object_name = blob.name
        file_uri = f'gs://{bucket_name}/{object_name}'

        print(f'Table {table_name} starting to be loaded')
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET

        load_job = bq_client.load_table_from_uri(
            file_uri,
            table_ref,
            job_config=job_config
        )
        load_job.result()
        print(f'Table {table_name} loaded successfully')