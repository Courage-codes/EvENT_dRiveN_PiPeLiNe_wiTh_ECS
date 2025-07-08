FROM bitnami/spark:3.5
USER root
WORKDIR /app

# Copy requirements first for better layer caching
COPY transformation-image/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY transformation-image/transformation.py .

# Set proper permissions
RUN chmod +x transformation.py

# Switch back to non-root user
USER 1001

# Set environment variables for better Spark performance
ENV SPARK_DRIVER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=2g
ENV SPARK_DRIVER_MAX_RESULT_SIZE=1g

CMD ["spark-submit", \
     "--packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.676", \
     "--conf", "spark.driver.host=127.0.0.1", \
     "--conf", "spark.hadoop.fs.s3a.path.style.access=true", \
     "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=true", \
     "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem", \
     "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain", \
     "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer", \
     "--conf", "spark.sql.adaptive.enabled=true", \
     "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true", \
     "--conf", "spark.driver.maxResultSize=1g", \
     "--conf", "spark.network.timeout=800s", \
     "--conf", "spark.executor.heartbeatInterval=60s", \
     "/app/transformation.py"]