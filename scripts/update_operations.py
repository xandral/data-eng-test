import psycopg2
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import socket
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters
db_params = {
    'dbname': 'crawl',
    'user': 'root',
    'password': 'secret',
    'host': 'postgres',
    'port': '5432'
}

# Connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **db_params)

def column_exists(cursor, table_name, column_name):
    query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = %s
        AND column_name = %s
    );
    """
    cursor.execute(query, (table_name, column_name))
    return cursor.fetchone()[0]

def add_column_if_not_exists(cursor, table_name, column_name, column_type):
    if not column_exists(cursor, table_name, column_name):
        add_column_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
        cursor.execute(add_column_query)
        logger.info(f"Added column: {column_name}")
    else:
        logger.info(f"Column {column_name} already exists.")

def get_batches(cursor, table_name, batch_size):
    cursor.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")
    min_id, max_id = cursor.fetchone()
    
    for start_id in range(min_id, max_id + 1, batch_size):
        end_id = start_id + batch_size - 1
        cursor.execute(f"SELECT id FROM {table_name} WHERE id BETWEEN %s AND %s ", (start_id, end_id))
        yield [row[0] for row in cursor.fetchall()]

def update_batch(batch, table_name, update_function):
    conn = connection_pool.getconn()
    cursor = conn.cursor()

    ids = tuple(batch)
    if ids:
        update_function(cursor, table_name, ids)
    
    conn.commit()
    cursor.close()
    connection_pool.putconn(conn)

def batch_update(table_name, batch_size, update_function):
    conn = connection_pool.getconn()
    cursor = conn.cursor()
    
    batches = list(get_batches(cursor, table_name, batch_size))
    cursor.close()
    connection_pool.putconn(conn)
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(update_batch, batch, table_name, update_function) for batch in batches]
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Update failed: {e}")

def add_flags_and_columns(table_name):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    columns_to_add = {
        'baseurl': 'TEXT',
        'domain': 'TEXT',
        'is_homepage': 'BOOLEAN',
        'country': 'TEXT',
        'content_type': 'TEXT',
        'is_ad_domain': 'BOOLEAN'
    }
    
    for column_name, column_type in columns_to_add.items():
        add_column_if_not_exists(cursor, table_name, column_name, column_type)
    
    conn.commit()
    cursor.close()
    conn.close()

def update_baseurl_and_homepage(cursor, table_name, ids):
    cursor.execute(f"""
        UPDATE {table_name}
        SET baseurl = SUBSTRING(url FROM 'https?://([^/]+)'),
            is_homepage = (url ~* 'https?://[^/]+/?$')
        WHERE id IN %s;
    """, (ids,))

def extract_suffix_from_domain(baseurl):
    """Extract domain suffix from baseurl."""
    domain_parts = baseurl.split('.')
    if len(domain_parts) > 1:
        return domain_parts[-1]  # Get the last part of the domain
    return None

def get_ip_from_domain(baseurl):
    try:
        return socket.gethostbyname(baseurl)
    except Exception as e:
        logger.error(f"Error resolving IP for {baseurl}: {e}")
        return None

def get_country(ip):
    try:
        response = requests.get(f"https://ipinfo.io/{ip}/country")
        if response.status_code == 200:
            return response.text.strip()
    except Exception as e:
        logger.error(f"Error getting country for IP {ip}: {e}")
    return None

def extract_country_from_domain(baseurl):
    """Extract country from domain suffixes where country is obvious."""
    known_country_codes = {
        'us': 'United States', 'uk': 'United Kingdom', 'ca': 'Canada', 'au': 'Australia', 'in': 'India', 'jp': 'Japan',
        # Add more country codes as needed
    }
    domain_suffix = extract_suffix_from_domain(baseurl)
    if domain_suffix and domain_suffix in known_country_codes:
        return known_country_codes[domain_suffix]
    return None

def update_country_for_baseurl(cursor, table_name, ids):
    cursor.execute(f"SELECT id, baseurl FROM {table_name} WHERE id IN %s", (ids,))
    rows = cursor.fetchall()

    for row in rows:
        id, baseurl = row
        country = extract_country_from_domain(baseurl)
        if not country:
            ip = get_ip_from_domain(baseurl)
            if ip:
                country = get_country(ip)

        if country:
            cursor.execute(f"UPDATE {table_name} SET country = %s WHERE id = %s", (country, id))
            logger.info(f"Updated country for ID {id} to {country}")

def update_data():
    table_name = 'urls'
    batch_size = 1000
    add_flags_and_columns(table_name)
    batch_update(table_name, batch_size, update_baseurl_and_homepage)
    batch_update(table_name, batch_size, update_country_for_baseurl)

if __name__ == "__main__":
    update_data()
