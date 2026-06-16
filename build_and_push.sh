#!/bin/bash

# ==============================================================================
# Smart Pull & Start Script for PySpark Platform (Optimized for Weak Internet)
# ==============================================================================

# Color coding for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting Pull and Compose Up pipeline for PySpark...${NC}"

# Function to retry commands on failure (very useful for weak/unstable internet)
retry_cmd() {
    local n=1
    local max_attempts=5
    local delay_sec=15
    while true; do
        echo -e "${BLUE}Running: ${NC}$*"
        "$@" && break || {
            if [[ $n -lt $max_attempts ]]; then
                echo -e "${YELLOW}⚠️ Action failed. Retrying ($n/$max_attempts) in $delay_sec seconds...${NC}"
                ((n++))
                sleep $delay_sec
            else
                echo -e "${RED}❌ Action failed after $max_attempts attempts.${NC}"
                return 1
            fi
        }
    done
}

# Go to script directory
cd "$(dirname "$0")" || exit 1

echo -n -e "\n${YELLOW}Do you want to pull and run all services or just the Spark stack? (all/spark) [all]: ${NC}"
read -r choice
choice=${choice:-all}

echo -n -e "\n${YELLOW}Do you want to build new images or just start existing ones? (build/up) [build]: ${NC}"
read -r action_choice
action_choice=${action_choice:-build}

BUILD_FLAG="--build"
if [[ "$action_choice" == "up" ]]; then
    BUILD_FLAG=""
fi

if [[ "$choice" == "spark" ]]; then
    if [[ "$action_choice" != "up" ]]; then
        echo -e "\n${BLUE}📥 Step 1: Pulling base and external images for Spark stack...${NC}"

    echo -e "${YELLOW}Pulling python:3.11-slim-bookworm (Spark Base Build)...${NC}"
    retry_cmd docker pull python:3.11-slim-bookworm || exit 1

    echo -e "${YELLOW}Pulling jupyter/pyspark-notebook:spark-3.5.0 (Notebook Build)...${NC}"
    retry_cmd docker pull jupyter/pyspark-notebook:spark-3.5.0 || exit 1

    echo -e "${YELLOW}Pulling postgres:16...${NC}"
    retry_cmd docker pull postgres:16 || exit 1

    echo -e "${YELLOW}Pulling minio/minio:latest...${NC}"
    retry_cmd docker pull minio/minio:latest || exit 1

    echo -e "${YELLOW}Pulling clickhouse/clickhouse-server:24.3-alpine...${NC}"
    retry_cmd docker pull clickhouse/clickhouse-server:24.3-alpine || exit 1

    echo -e "${YELLOW}Pulling projectnessie/nessie:latest...${NC}"
    retry_cmd docker pull projectnessie/nessie:latest || exit 1

    echo -e "${YELLOW}Pulling dremio/dremio-oss:latest...${NC}"
    retry_cmd docker pull dremio/dremio-oss:latest || exit 1
    else
        echo -e "\n${BLUE}⏩ Skipping image pull because 'up' action was selected...${NC}"
    fi

    echo -e "\n${BLUE}⚙️ Step 2: Starting the Spark stack...${NC}"
    docker compose up -d $BUILD_FLAG postgres minio spark-master spark-worker-1 spark-worker-2 jupyter-workspace spark-history nessie dremio clickhouse || {
        echo -e "${RED}❌ Failed to start the Spark platform via docker compose.${NC}"
        exit 1
    }
else
    if [[ "$action_choice" != "up" ]]; then
        # 1. Pull All Base and External Images (with retry)
        echo -e "\n${BLUE}📥 Step 1: Pulling all base and external images from Docker Hub...${NC}"

    # Base build images
    echo -e "${YELLOW}Pulling python:3.11-slim-bookworm (Spark Base Build)...${NC}"
    retry_cmd docker pull python:3.11-slim-bookworm || exit 1

    echo -e "${YELLOW}Pulling jupyter/pyspark-notebook:spark-3.5.0 (Notebook Build)...${NC}"
    retry_cmd docker pull jupyter/pyspark-notebook:spark-3.5.0 || exit 1

    # Database and Storage images
    echo -e "${YELLOW}Pulling postgres:16...${NC}"
    retry_cmd docker pull postgres:16 || exit 1

    echo -e "${YELLOW}Pulling minio/minio:latest...${NC}"
    retry_cmd docker pull minio/minio:latest || exit 1

    echo -e "${YELLOW}Pulling clickhouse/clickhouse-server:24.3-alpine...${NC}"
    retry_cmd docker pull clickhouse/clickhouse-server:24.3-alpine || exit 1

    # Kafka and Confluent stack images
    echo -e "${YELLOW}Pulling confluentinc/cp-zookeeper:7.8.0...${NC}"
    retry_cmd docker pull confluentinc/cp-zookeeper:7.8.0 || exit 1

    echo -e "${YELLOW}Pulling confluentinc/cp-kafka:7.8.0...${NC}"
    retry_cmd docker pull confluentinc/cp-kafka:7.8.0 || exit 1

    echo -e "${YELLOW}Pulling confluentinc/cp-kafka-rest:7.8.0...${NC}"
    retry_cmd docker pull confluentinc/cp-kafka-rest:7.8.0 || exit 1

    echo -e "${YELLOW}Pulling confluentinc/cp-kafka-connect:7.8.0...${NC}"
    retry_cmd docker pull confluentinc/cp-kafka-connect:7.8.0 || exit 1

    echo -e "${YELLOW}Pulling confluentinc/cp-ksqldb-server:7.8.0...${NC}"
    retry_cmd docker pull confluentinc/cp-ksqldb-server:7.8.0 || exit 1

    # Other platform engines
    echo -e "${YELLOW}Pulling projectnessie/nessie:latest...${NC}"
    retry_cmd docker pull projectnessie/nessie:latest || exit 1

    echo -e "${YELLOW}Pulling dremio/dremio-oss:latest...${NC}"
    retry_cmd docker pull dremio/dremio-oss:latest || exit 1

    echo -e "${YELLOW}Pulling conduktor/conduktor-console:1.44.1...${NC}"
    retry_cmd docker pull conduktor/conduktor-console:1.44.1 || exit 1

    echo -e "${YELLOW}Pulling apache/airflow:3.2.1-python3.12 (Base Airflow Image)...${NC}"
    retry_cmd docker pull apache/airflow:3.2.1-python3.12 || exit 1

    echo -e "${YELLOW}Pulling redis:7.2-bookworm...${NC}"
    retry_cmd docker pull redis:7.2-bookworm || exit 1

    echo -e "${YELLOW}Pulling redis/redisinsight:latest...${NC}"
    retry_cmd docker pull redis/redisinsight:latest || exit 1

    echo -e "${YELLOW}Pulling confluentinc/cp-schema-registry:7.8.0...${NC}"
    retry_cmd docker pull confluentinc/cp-schema-registry:7.8.0 || exit 1

    echo -e "${YELLOW}Pulling postgres:16-alpine (Conduktor Metadata DB)...${NC}"
    retry_cmd docker pull postgres:16-alpine || exit 1

    echo -e "${YELLOW}Pulling prom/prometheus:v2.51.2...${NC}"
    retry_cmd docker pull prom/prometheus:v2.51.2 || exit 1

    echo -e "${YELLOW}Pulling grafana/grafana:10.4.2...${NC}"
    retry_cmd docker pull grafana/grafana:10.4.2 || exit 1

    echo -e "${YELLOW}Pulling docker.elastic.co/elasticsearch/elasticsearch:8.10.2...${NC}"
    retry_cmd docker pull docker.elastic.co/elasticsearch/elasticsearch:8.10.2 || exit 1

    echo -e "${YELLOW}Pulling openmetadata/server:1.3.1...${NC}"
    retry_cmd docker pull openmetadata/server:1.3.1 || exit 1

    echo -e "${YELLOW}Pulling traefik:v3.3...${NC}"
    retry_cmd docker pull traefik:v3.3 || exit 1

    echo -e "${YELLOW}Pulling apache/superset:3.1.3...${NC}"
    retry_cmd docker pull apache/superset:3.1.3 || exit 1
    else
        echo -e "\n${BLUE}⏩ Skipping image pull because 'up' action was selected...${NC}"
    fi

    # 2. Start all services using Docker Compose (automatically builds local images using the pulled base images)
    echo -e "\n${BLUE}⚙️ Step 2: Starting the platform...${NC}"
    docker compose up -d $BUILD_FLAG || {
        echo -e "${RED}❌ Failed to start the platform via docker compose.${NC}"
        exit 1
    }
fi

echo -e "\n${GREEN}🎉 All done successfully! All images pulled, built, and services are running in the background.${NC}"
