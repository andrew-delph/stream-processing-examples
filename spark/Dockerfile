# Dockerfile
FROM bitnami/spark:3

COPY app.py /app/

CMD ["/opt/bitnami/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2", "/app/app.py"]
