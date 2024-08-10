import pyarrow as pa
import pandas as pd
from warcio.archiveiterator import ArchiveIterator
import re
from urllib.parse import urlparse
import os

def is_external(url, base_domain):
    try:
        parsed_url = urlparse(url)
        return parsed_url.netloc and parsed_url.netloc != base_domain
    except Exception as e:
        return False

def extract_external_urls(warc_file_path):
    external_urls = set()  # Use a set to avoid duplicate URLs

    with open(warc_file_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                base_url = record.rec_headers.get_header('WARC-Target-URI')
                if base_url:
                    base_domain = urlparse(base_url).netloc
                    content = record.content_stream().read().decode('utf-8', errors='ignore')
                    links = re.findall(r'href=["\'](http[s]?://[^"\']+)["\']', content)
                    for url in links:
                        if is_external(url, base_domain):
                            external_urls.add(url)

    return external_urls

def save_urls_to_arrow(urls, arrow_file_path):
    # Create a PyArrow table
    table = pa.Table.from_pandas(pd.DataFrame({'url': list(urls)}))
    
    # Write the table to an Arrow file
    with pa.OSFile(arrow_file_path, 'wb') as sink:
        with pa.RecordBatchFileWriter(sink, table.schema) as writer:
            writer.write_table(table)

def decode_data():
    warc_folder_path = './scripts/raw_data'
    all_urls = set()
    
    # List all files in the specified folder
    for filename in os.listdir(warc_folder_path):
        warc_file_path = os.path.join(warc_folder_path, filename)
        if os.path.isfile(warc_file_path): 
            urls = extract_external_urls(warc_file_path)
            all_urls.update(urls)
    
    arrow_file_path = 'scripts/data/urls.arrow'
    save_urls_to_arrow(all_urls, arrow_file_path)

if __name__ == '__main__':
    decode_data()
