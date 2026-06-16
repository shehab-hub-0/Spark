import os

REPLACEMENTS = {
    "postgres": "postgres",
    "redisinsight": "redisinsight",
    "redis": "redis",
    "minio": "minio",
    "zookeeper": "zookeeper",
    "kafka-broker": "kafka-broker",
    "schema-registry": "schema-registry",
    "kafka-rest": "kafka-rest",
    "kafka-connect": "kafka-connect",
    "ksqldb": "ksqldb",
    "conduktor-db": "conduktor-db",
    "conduktor": "conduktor",
    "spark-master": "spark-master",
    "spark-worker-1": "spark-worker-1",
    "spark-worker-2": "spark-worker-2",
    "jupyter-workspace": "jupyter-workspace",
    "spark-history": "spark-history",
    "nessie": "nessie",
    "dremio": "dremio",
    "clickhouse": "clickhouse",
    "prometheus": "prometheus",
    "grafana": "grafana",
    "elasticsearch": "elasticsearch",
    "kibana": "kibana",
    "openmetadata": "openmetadata",
    "traefik": "traefik",
    "airflow-web": "airflow-web",
    "airflow-scheduler": "airflow-scheduler",
    "airflow-worker": "airflow-worker",
    "airflow-triggerer": "airflow-triggerer",
    "airflow-init": "airflow-init",
    "airflow-cli": "airflow-cli"
}

def replace_in_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = content
    # Using sorted keys by length in reverse order ensures we replace longer names first
    # e.g., 'redisinsight' before 'redis'
    for old, new in sorted(REPLACEMENTS.items(), key=lambda x: len(x[0]), reverse=True):
        new_content = new_content.replace(old, new)

    if new_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated: {file_path}")

def main():
    root_dir = "/home/shehab/Downloads/work/pyspark"
    
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Skip .git directory and hidden files
        if '.git' in dirpath:
            continue
        
        for filename in filenames:
            # Only process files with specific extensions we care about to avoid binaries
            if filename.endswith(('.yml', '.yaml', '.sh', '.conf', '.py', '.ipynb', '.md', '.sql', '.properties', '.cfg', '.xml')):
                file_path = os.path.join(dirpath, filename)
                try:
                    replace_in_file(file_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    main()
