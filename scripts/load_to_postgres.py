import pyarrow as pa
import psycopg2
import psycopg2.extras

def load_arrow_to_postgres(arrow_file_path, db_params, table_name, chunk_size=1000):
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Open the Arrow file
    with pa.memory_map(arrow_file_path, 'r') as source:
        reader = pa.ipc.RecordBatchFileReader(source)
        
        # Create table if it does not exist
        first_batch = reader.get_batch(0)
        columns = ', '.join([f"{field.name} TEXT" for field in first_batch.schema])  # Adjust type based on your data
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id serial, {columns});"
        cursor.execute(create_table_query)

        # Process each record batch in the Arrow file
        for i in range(reader.num_record_batches):
            batch = reader.get_batch(i)
            df = batch.to_pandas()
            
            # Insert data in chunks
            for start in range(0, len(df), chunk_size):
                end = min(start + chunk_size, len(df))
                chunk = df.iloc[start:end]
                tuples = [tuple(row) for row in chunk.itertuples(index=False)]
                insert_query = f"INSERT INTO {table_name} ({', '.join(chunk.columns)}) VALUES %s"
                psycopg2.extras.execute_values(cursor, insert_query, tuples)
            
            conn.commit()

    # Close connection
    cursor.close()
    conn.close()

def load_to_postgres():
    arrow_file_path = 'scripts/data/urls.arrow'
    db_params = {
        'dbname': 'crawl',
        'user': 'root',
        'password': 'secret',
        'host': 'postgres',
        'port': '5432'
    }
    table_name = 'urls'
    load_arrow_to_postgres(arrow_file_path, db_params, table_name)


if __name__ == '__main__':
    load_to_postgres()