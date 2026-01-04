FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Install Python dependencies globally
# --break-system-packages is needed on newer distributions to bypass PEP 668
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    happybase==1.3.0 \
    kafka-python==2.0.2 \
    vaderSentiment==3.3.2

# Prefetch Spark dependencies (Kafka connector, etc.) to avoid runtime network collisions
# We run a dummy SparkPi job just to trigger the Ivy/Maven download.
RUN /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_*.jar 1

# Set ownership of work directories to spark user
RUN mkdir -p /home/spark/.ivy2 /home/spark/.cache && \
    chown -R spark:spark /home/spark

USER spark
