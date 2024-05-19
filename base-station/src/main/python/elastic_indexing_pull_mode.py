import pandas as pd
from elasticsearch import Elasticsearch, helpers
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import time
import pyarrow.parquet as pq
import pyarrow as pa

# A set to keep track of already processed files
processed_files = set()

def read_parquet_file(file_path):
    # Read the Parquet file into a Pandas DataFrame using pyarrow
    table = pq.read_table(file_path)
    df = table.to_pandas()
    return df

def connect_elasticsearch():
    es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
    if es.ping():
        print('Connected to Elasticsearch')
    else:
        print('Could not connect to Elasticsearch')
    return es

def index_data(es, df, index_name):
    # Convert DataFrame to JSON format
    records = df.to_dict(orient='records')

    # Prepare actions for bulk indexing
    actions = [
        {
            "_index": index_name,
            "_source": record
        }
        for record in records
    ]

    # Perform bulk indexing
    helpers.bulk(es, actions)
    print(f"Indexed {len(records)} records into {index_name}")

def process_file(es, file_path):
    if file_path not in processed_files:
        if is_file_complete(file_path):
            df = read_parquet_file(file_path)
            # Extract station_id and date from the file path
            parts = file_path.split(os.sep)
            station_id = parts[-3]
            date = parts[-2]
            index_name = f"{station_id}_{date}"
            index_data(es, df, index_name)
            # Mark the file as processed
            processed_files.add(file_path)

def is_file_complete(file_path, wait_time=0.1):
    """
    Check if a file is completely written by attempting to read its footer.
    :param file_path: Path to the file.
    :param wait_time: Time to wait between retries in seconds.
    :param max_retries: Maximum number of retries.
    :return: True if the file is complete, False otherwise.
    """
    while True:
        try:
            # Attempt to read the file footer
            pq.read_metadata(file_path)
            return True
        except pa.lib.ArrowInvalid:
            # File is still being written; wait and retry
            time.sleep(wait_time)

class ParquetFileHandler(FileSystemEventHandler):
    def __init__(self, es):
        self.es = es

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.parquet'):
            process_file(self.es, event.src_path)

if __name__ == "__main__":
    root_dir = 'Weather-Stations-Monitoring/base-station/src/main/resources/archiving_files/archive'
    es = connect_elasticsearch()

    # Process existing files in the directory initially
    for station_dir in os.listdir(root_dir):
        station_path = os.path.join(root_dir, station_dir)
        if os.path.isdir(station_path):
            for date_dir in os.listdir(station_path):
                date_path = os.path.join(station_path, date_dir)
                if os.path.isdir(date_path):
                    for file_name in os.listdir(date_path):
                        if file_name.endswith('.parquet'):
                            file_path = os.path.join(date_path, file_name)
                            process_file(es, file_path)

    # Set up the watchdog observer to monitor the directory for new files
    event_handler = ParquetFileHandler(es)
    observer = Observer()
    observer.schedule(event_handler, path=root_dir, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
