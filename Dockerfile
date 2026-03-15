# =============================================================================
# Production-Grade PySpark Dev Container
# Base: apache/spark:3.5.0
# Architecture: Single-Node (Local Mode)
# Target: VS Code Dev Containers
# =============================================================================

FROM apache/spark:3.5.0

# -----------------------------------------------------------------------------
# Switch to root for system-level installations
# -----------------------------------------------------------------------------
USER root

# -----------------------------------------------------------------------------
# Build arguments
# -----------------------------------------------------------------------------
ARG PYSPARK_VERSION=3.5.0
ARG PYTHON_VERSION=3.11

# -----------------------------------------------------------------------------
# Environment variables
# -----------------------------------------------------------------------------
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:/usr/local/bin:/usr/bin:/bin:${PATH}"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_NO_DAEMONIZE=true

# Disable Spark's noisy startup messages for cleaner dev experience
ENV SPARK_LOCAL_IP=127.0.0.1

# -----------------------------------------------------------------------------
# Install system dependencies required by VS Code Dev Containers
# and PySpark development tooling
# -----------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Core VS Code Dev Container requirements
    curl \
    wget \
    git \
    git-lfs \
    openssh-client \
    gnupg2 \
    # Build tools for Python packages
    build-essential \
    python3-dev \
    python3-pip \
    python3-venv \
    # Process & shell utilities
    procps \
    lsof \
    htop \
    less \
    vim \
    nano \
    # Locale support
    locales \
    # sudo support (VS Code Dev Containers expects this)
    sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && locale-gen en_US.UTF-8

ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# -----------------------------------------------------------------------------
# Ensure Python 3 is the system default
# -----------------------------------------------------------------------------
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1 \
    && update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# -----------------------------------------------------------------------------
# Install PySpark — MUST match Spark 3.5.0 exactly
# Install essential data engineering Python packages
# -----------------------------------------------------------------------------
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
    # PySpark — pinned to exact Spark version (CRITICAL: must match 3.5.0)
    pyspark==3.5.0 \
    # Type stubs for IDE IntelliSense support
    pyspark-stubs \
    # Data manipulation
    pandas==2.1.4 \
    pyarrow==14.0.2 \
    numpy==1.26.4 \
    # Development & quality tools
    black \
    isort \
    flake8 \
    mypy \
    pytest \
    pytest-cov \
    # Utility
    python-dotenv \
    rich

# -----------------------------------------------------------------------------
# Create a non-root developer user that VS Code will use
# Username: spark (matches the base image convention)
# UID/GID: 1000 (standard VS Code Dev Container UID)
# -----------------------------------------------------------------------------
RUN groupadd --gid 1000 spark 2>/dev/null || true && \
    useradd \
        --uid 1000 \
        --gid 1000 \
        --create-home \
        --shell /bin/bash \
        --comment "Spark Developer" \
        sparkdev 2>/dev/null || true && \
    # Grant passwordless sudo for dev convenience
    echo "sparkdev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/sparkdev && \
    chmod 0440 /etc/sudoers.d/sparkdev

# -----------------------------------------------------------------------------
# Fix ownership so the sparkdev user can access Spark internals
# -----------------------------------------------------------------------------
RUN chown -R sparkdev:1000 /opt/spark && \
    chmod -R 755 /opt/spark

# -----------------------------------------------------------------------------
# Create workspace and Spark working directories with correct permissions
# -----------------------------------------------------------------------------
RUN mkdir -p /workspace \
             /tmp/spark-events \
             /tmp/spark-warehouse \
             /tmp/spark-checkpoints \
             /home/sparkdev/.ivy2 \
             /home/sparkdev/.m2 && \
    chown -R sparkdev:1000 \
        /workspace \
        /tmp/spark-events \
        /tmp/spark-warehouse \
        /tmp/spark-checkpoints \
        /home/sparkdev

# -----------------------------------------------------------------------------
# Copy Spark configuration optimized for 12GB RAM / 6 CPU single-node setup
# -----------------------------------------------------------------------------
COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY conf/log4j2.properties ${SPARK_HOME}/conf/log4j2.properties

RUN chown sparkdev:1000 \
        ${SPARK_HOME}/conf/spark-defaults.conf \
        ${SPARK_HOME}/conf/log4j2.properties

# -----------------------------------------------------------------------------
# Switch to the developer user for all subsequent operations
# -----------------------------------------------------------------------------
USER sparkdev
WORKDIR /workspace

# -----------------------------------------------------------------------------
# Configure bash for developer experience
# -----------------------------------------------------------------------------
RUN echo '' >> /home/sparkdev/.bashrc && \
    echo '# ── PySpark Dev Environment ──────────────────────────────────────' >> /home/sparkdev/.bashrc && \
    echo 'export SPARK_HOME=/opt/spark' >> /home/sparkdev/.bashrc && \
    echo 'export PYSPARK_PYTHON=python3' >> /home/sparkdev/.bashrc && \
    echo 'export PYSPARK_DRIVER_PYTHON=python3' >> /home/sparkdev/.bashrc && \
    echo 'export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"' >> /home/sparkdev/.bashrc && \
    echo 'export PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"' >> /home/sparkdev/.bashrc && \
    echo 'alias ll="ls -alF --color=auto"' >> /home/sparkdev/.bashrc && \
    echo 'alias pyspark-shell="pyspark --master local[*]"' >> /home/sparkdev/.bashrc && \
    echo 'echo "🚀 PySpark 3.5.0 Dev Container ready. Run: python src/main.py"' >> /home/sparkdev/.bashrc

# -----------------------------------------------------------------------------
# Keep container alive: VS Code attaches to a running container.
# sleep infinity is the canonical, zero-overhead approach.
# -----------------------------------------------------------------------------
CMD ["sleep", "infinity"]
