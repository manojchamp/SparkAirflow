import sys
import pandas as pd
from pyspark.sql import SparkSession

def process_kafka_data(csv_file_path):
    """Process the Kafka data stored in a CSV file."""
    spark = SparkSession.builder.appName("KafkaToPostgres").getOrCreate()

    # Read the CSV file into a Spark DataFrame
    df = spark.read.csv(csv_file_path, header=False, inferSchema=True)
    df.show()  # Print data (you can perform transformations here)

    # Write data to PostgreSQL
    df.write.jdbc(url="jdbc:postgresql://103.212.121.35:5433/nmsdb", 
                  table="nms_snmp_ifadminstatus_sample", 
                  mode="append", 
                  properties={"user": "<username>", "password": "<password>", "driver": "org.postgresql.Driver"})

    spark.stop()

if __name__ == "__main__":
    csv_file_path = sys.argv[1]  # Get CSV file path passed from Airflow
    process_kafka_data(csv_file_path)
