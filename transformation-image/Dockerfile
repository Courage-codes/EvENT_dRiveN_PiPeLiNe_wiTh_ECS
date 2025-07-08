FROM bitnami/spark:3.5
USER root
WORKDIR /app
COPY transformation-image/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY transformation-image/transformation.py .
USER 1001
CMD ["spark-submit", \
"--packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.676", \
"--conf", "spark.driver.host=127.0.0.1", \
"--conf", "spark.hadoop.fs.s3a.path.style.access=true", \
"--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=true", \
"/app/transformation.py"]