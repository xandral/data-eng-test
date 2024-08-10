import pandas as pd
import psycopg2
import json

# Database connection parameters
db_params = {
    'dbname': 'crawl',
    'user': 'root',
    'password': 'secret',
    'host': '127.0.0.1',
    'port': '5433'
}

def execute_query(query, params=None):
    """Executes a SQL query and returns the result as a DataFrame."""
    with psycopg2.connect(**db_params) as conn:
        df = pd.read_sql(query, conn, params=params)
    return df

def compute_metrics():
    metrics = {}

    # 1. Number of Unique Primary Links
    unique_primary_links_query = """
    SELECT COUNT(DISTINCT baseurl) AS unique_primary_links
    FROM urls;
    """
    unique_primary_links = execute_query(unique_primary_links_query).iloc[0]['unique_primary_links']
    metrics['Number of Unique Primary Links'] = int(unique_primary_links)

    # 2. Average Frequency of Primary Links
    avg_frequency_query = """
    SELECT AVG(frequency) AS avg_frequency
    FROM (
        SELECT baseurl, COUNT(*) AS frequency
        FROM urls
        GROUP BY baseurl
    ) AS link_frequencies;
    """
    avg_frequency = execute_query(avg_frequency_query).iloc[0]['avg_frequency']
    metrics['Average Frequency of Primary Links'] = float(avg_frequency) if avg_frequency is not None else None

    # 3. Number of Home Pages vs. Subsections
    homepage_vs_subsections_query = """
    SELECT
        SUM(CASE WHEN is_homepage THEN 1 ELSE 0 END) AS homepage_count,
        SUM(CASE WHEN NOT is_homepage THEN 1 ELSE 0 END) AS subsection_count
    FROM urls;
    """
    homepage_vs_subsections = execute_query(homepage_vs_subsections_query).iloc[0]
    metrics['Number of Home Pages'] = int(homepage_vs_subsections['homepage_count'])
    metrics['Number of Subsections'] = int(homepage_vs_subsections['subsection_count'])

    # 4. Distribution of Content Types
    content_type_distribution_query = """
    SELECT content_type, COUNT(*) AS count
    FROM urls
    GROUP BY content_type
    ORDER BY count DESC;
    """
    content_type_distribution = execute_query(content_type_distribution_query)
    metrics['Content Type Distribution'] = content_type_distribution.set_index('content_type').to_dict()['count']
    
    # Convert content type distribution values to int
    metrics['Content Type Distribution'] = {k: int(v) for k, v in metrics['Content Type Distribution'].items()}

    # 5. Geographic Distribution (Countries) of Links
    geographic_distribution_query = """
    SELECT country, COUNT(*) AS count
    FROM urls
    GROUP BY country
    ORDER BY count DESC;
    """
    geographic_distribution = execute_query(geographic_distribution_query)
    metrics['Geographic Distribution'] = geographic_distribution.set_index('country').to_dict()['count']

    # Convert geographic distribution values to int
    metrics['Geographic Distribution'] = {k: int(v) for k, v in metrics['Geographic Distribution'].items()}

    return metrics

def save_metrics_to_json(metrics, file_path):
    """Saves the metrics dictionary to a JSON file."""
    with open(file_path, 'w') as f:
        json.dump(metrics, f, indent=4)

if __name__ == "__main__":
    metrics = compute_metrics()
    file_path='metric_results.json'
    save_metrics_to_json(metrics, file_path)
    print("Metrics saved to metrics_results.json")
