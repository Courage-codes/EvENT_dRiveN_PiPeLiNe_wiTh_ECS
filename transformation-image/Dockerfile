# 1. Use the official Bitnami Spark image as a base.
FROM bitnami/spark:3.5

# 2. Switch to the 'root' user to install packages.
USER root

# 3. Set working directory for your app.
WORKDIR /app

# 4. Copy only requirements.txt first for better layer caching.
COPY transformation-image/requirements.txt .

# 5. Install Python packages from requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy your application code.
COPY transformation-image/transformation.py .

# 7. (Optional) Fix permissions if needed.
# RUN chown -R 1001:1001 /app

# 8. Switch back to the non-root 'spark' user for security.
USER 1001

# 9. Set the default command to run your Spark job.
CMD ["spark-submit", \
     "--packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "--conf", "spark.driver.host=127.0.0.1", \
     "/app/transformation.py"]
