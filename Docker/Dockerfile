# This Dockerfile is taken from https://github.com/mvillarrealb/docker-spark-cluster/blob/master/docker-compose.yml
# and updated to install the Scala 2.13 version of Spark 3.2.0

FROM eclipse-temurin:17-jre-jammy as builder

# Add Dependencies for PySpark

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=3.5.5 \
SCALA_VERSION=2.13 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz


# PostgreSQL JDBC driver
RUN curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
      -o ${SPARK_HOME}/jars/postgresql-42.7.4.jar

#AWS3
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar \
 && curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar

FROM builder as apache-spark

# Python runtime minimal 3.11 pour 
RUN apt-get update && apt-get install -y \
    python3.11 python3.11-venv python3.11-distutils python3-pip \
 && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
 && rm -rf /var/lib/apt/lists/*


WORKDIR /app

# Installer uv dans le builder
RUN pip install --no-cache-dir uv

# Copier uniquement les fichiers de dépendances
COPY pyproject.toml uv.lock* ./

# Créer le venv et installer les deps
RUN uv sync --frozen --no-dev

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000

EXPOSE 8080 7077 7000

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY start-spark.sh /

# Exposer le port SSH
EXPOSE 22

CMD ["/bin/bash", "/start-spark.sh"]
