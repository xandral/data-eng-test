import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database connection parameters
db_params = {
    'dbname': 'crawl',
    'user': 'root',
    'password': 'secret',
    'host': 'localhost',
    'port': '5433'
}

# Number of rows per batch
BATCH_SIZE = 10000

# Connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **db_params)

def fetch_batch(cursor, table_name, start_id, end_id):
    """Fetches a batch of rows from the database."""
    query = f"SELECT * FROM {table_name} WHERE id BETWEEN %s AND %s;"
    cursor.execute(query, (start_id, end_id))
    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    return df

def save_batch_to_arrow(df, batch_number, arrow_file_path):
    """Saves a batch DataFrame to an Arrow file."""
    table = pa.Table.from_pandas(df)
    if batch_number == 0:
        with pa.OSFile(arrow_file_path, 'wb') as sink:
            with pa.RecordBatchFileWriter(sink, table.schema) as writer:
                writer.write_table(table)
    else:
        with pa.OSFile(arrow_file_path, 'ab') as sink:
            with pa.RecordBatchFileWriter(sink, table.schema) as writer:
                writer.write_table(table)

def process_batches(table_name, batch_size, arrow_file_path):
    """Processes and saves batches in parallel."""
    conn = connection_pool.getconn()
    cursor = conn.cursor()

    cursor.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")
    min_id, max_id = cursor.fetchone()

    batch_ranges = [(start_id, min(start_id + batch_size - 1, max_id)) for start_id in range(min_id, max_id + 1, batch_size)]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for i, (start_id, end_id) in enumerate(batch_ranges):
            futures.append(executor.submit(fetch_and_save_batch, table_name, start_id, end_id, arrow_file_path, i))
        
        for future in as_completed(futures):
            future.result()

    cursor.close()
    connection_pool.putconn(conn)

def fetch_and_save_batch(table_name, start_id, end_id, arrow_file_path, batch_number):
    """Fetches a batch and saves it to the Arrow file."""
    conn = connection_pool.getconn()
    cursor = conn.cursor()
    df = fetch_batch(cursor, table_name, start_id, end_id)
    save_batch_to_arrow(df, batch_number, arrow_file_path)
    cursor.close()
    connection_pool.putconn(conn)

if __name__ == "__main__":
    table_name = 'urls'
    arrow_file_path = 'large_table.arrow'
    process_batches(table_name, BATCH_SIZE, arrow_file_path)
    print(f"Data saved to {arrow_file_path}")
