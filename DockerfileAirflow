FROM apache/airflow:2.9.3-python3.11

# ---- System deps for PySpark ----
USER root
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      procps \
 && rm -rf /var/lib/apt/lists/*

# ---- Java env vars ----
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN ln -s /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk || true
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# ---- Constraints (исправленный URL) ----
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ENV CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# ---- Providers под constraints ----
RUN pip install --no-cache-dir -c "${CONSTRAINTS_URL}" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-spark

# ---- PySpark + dbt ----
RUN pip install --no-cache-dir \
    pyspark==3.5.2 \
    psycopg2-binary==2.9.9 \
    dbt-core \
    dbt-postgres

# ---- DAGs ----
ENV PYTHONPATH="/opt/airflow/dags:${PYTHONPATH}"