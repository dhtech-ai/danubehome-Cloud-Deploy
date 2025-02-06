import json
import datetime
import os
import re
from google.cloud import storage, bigquery
import pyodbc

# GCS and BigQuery configuration
raw_bucket_name = "raw_staging"
archive_bucket_name = "archive_dh"
folder_path = "sql_ingestion/ls/"
bigquery_dataset = "danubehome-dlf-prd.raw"
bigquery_table_prefix = "ls_"

# Current timestamp for file naming and archival
current_date_time = datetime.datetime.now().strftime('%Y%m%d-%H%M')
ingest_time = datetime.datetime.now().isoformat()

# Function to normalize column names
def normalize_column_name(column_name):
    column_name = re.sub(r"[^0-9a-zA-Z_]", "_", column_name)  # Replace invalid characters
    if not re.match(r"^[a-zA-Z_]", column_name):  # Ensure it starts with a letter or underscore
        column_name = f"col_{column_name}"
    return column_name[:300]  # Limit to 300 characters

# Generate dynamic schema with normalized columns
def generate_dynamic_schema(data):
    if not data:
        return []
    columns = [normalize_column_name(col) for col in data[0].keys()]
    return [bigquery.SchemaField(column, "STRING") for column in columns]

# Step 1: Extract and upload to GCS
def extract_and_upload_to_gcs(tables):
    try:
        storage_client = storage.Client()
        raw_bucket = storage_client.bucket(raw_bucket_name)

        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 18 for SQL Server};'
            'SERVER=192.168.155.81;'  # Replace with your server name
            'DATABASE=BI_DATABASE;'  # Replace with your database name
            'UID=Reports;'  # Replace with your username
            'PWD=Planning@Living;'  # Replace with your password
            'TrustServerCertificate=yes;'
        )

        for table in tables:
            print(f"[INFO] Extracting table: {table}")
            gcs_file_path = f"{folder_path}{table}.json"

            cursor = connection.cursor()
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
            columns = [row[0] for row in cursor.fetchall()]

            # normalized_column_names = [normalize_column_name(col) for col in columns]

            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            print(f"[DEBUG] Extracted {len(rows)} rows from table {table}.")

            temp_dir = "tmp/"
            os.makedirs(temp_dir, exist_ok=True)
            temp_file_path = os.path.join(temp_dir, f"{table}_extracted_data.json")

            with open(temp_file_path, "w", encoding="utf-8") as file:
                for row in rows:
                    row_data = {normalize_column_name(columns[i]): str(row[i]) if row[i] is not None else "" for i in range(len(columns))}
                    row_data["ingest_time"] = ingest_time
                    json.dump(row_data, file, ensure_ascii=False)
                    file.write("\n")

            blob = raw_bucket.blob(gcs_file_path)
            blob.upload_from_filename(temp_file_path)
            print(f"[INFO] Uploaded {table} to GCS at {gcs_file_path}")

        connection.close()
    except Exception as e:
        print(f"[ERROR] Failed to extract and upload tables: {e}")

# Step 2: Load from GCS to BigQuery
def load_from_gcs_to_bigquery():
    try:
        storage_client = storage.Client()
        bigquery_client = bigquery.Client()

        blobs = storage_client.list_blobs(raw_bucket_name, prefix=folder_path)
        for blob in blobs:
            if blob.name.endswith(".json"):
                table_name = f"{bigquery_table_prefix}{os.path.basename(blob.name).split('.')[0]}"
                print(f"[INFO] Loading data from GCS to BigQuery: {blob.name} -> {table_name}")

                data = blob.download_as_text()
                records = [json.loads(line) for line in data.strip().split("\n")]

                # Normalize column names in the records
                records = [{normalize_column_name(k): v for k, v in record.items()} for record in records]

                # Generate schema
                schema = generate_dynamic_schema(records)
                temp_file_path = "cleaned_data.json"

                # Write normalized data to temporary file
                with open(temp_file_path, "w") as temp_file:
                    for record in records:
                        json.dump(record, temp_file)
                        temp_file.write("\n")

                table_id = f"{bigquery_dataset}.{table_name}"
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )

                with open(temp_file_path, "rb") as file:
                    load_job = bigquery_client.load_table_from_file(file, table_id, job_config=job_config)
                load_job.result()

                print(f"[INFO] Successfully loaded {table_name} to BigQuery")

    except Exception as e:
        print(f"[ERROR] Failed to load data to BigQuery: {e}")

# Step 3: Move files to the archive bucket
def move_files_to_archive():
    try:
        storage_client = storage.Client()
        raw_bucket = storage_client.bucket(raw_bucket_name)
        archive_bucket = storage_client.bucket(archive_bucket_name)
        blobs = storage_client.list_blobs(raw_bucket_name, prefix=folder_path)

        for blob in blobs:
            destination_file_path = f"sql_ingestion/ls/{current_date_time}/{blob.name.split('/')[-1]}"
            source_blob = raw_bucket.blob(blob.name)
            archive_bucket.copy_blob(source_blob, archive_bucket, destination_file_path)
            source_blob.delete()
            print(f"[INFO] File {blob.name} moved to archive at {destination_file_path}")
    except Exception as e:
        print(f"[ERROR] Failed to archive files: {e}")

# Main function
def main():
    tables_to_extract = ["Item_Hierarchy","ColorCodes","ItemType","Lifestyle","PriceBand","StoreTier","Vendors","ItemUDA","Store_Master","Division"]  # Replace with your table names

    print("[INFO] Starting extraction and upload to GCS...")
    extract_and_upload_to_gcs(tables_to_extract)
    print("[INFO] Extraction and GCS upload completed.")

    print("[INFO] Starting load from GCS to BigQuery...")
    load_from_gcs_to_bigquery()
    print("[INFO] Data successfully loaded to BigQuery.")

    print("[INFO] Starting archival of files from raw bucket to archive bucket...")
    move_files_to_archive()
    print("[INFO] File archival completed.")

if __name__ == "__main__":
    main()
