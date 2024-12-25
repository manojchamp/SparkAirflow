import json
import csv
import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

KAFKA_TOPIC = "if_admin_status_info"
TMP_CSV_PATH = "/tmp/kafka_data.csv"

def consume_function(message, **kwargs):
    """Consumes messages from the Kafka topic and converts nested JSON to CSV format."""
    try:
        # Attempt to decode the value
        raw_value = message.value()
        value = json.loads(raw_value)
    except json.JSONDecodeError as e:
        value = raw_value  # Use raw value if decoding fails
        print(f"Error decoding value: {e}. Raw value: {raw_value}")

    print(f"Received Message: Value={value}")

    # Flatten the JSON and extract necessary fields
    flattened_data = {
        "neKey": value["ifAdminStatusPK"]["neKey"],
        "trapOid": value["ifAdminStatusPK"]["trapOid"],
        "updatedOn": value["ifAdminStatusPK"]["updatedOn"],
        "paramName": value["paramName"],
        "paramValue": value["paramValue"]
    }

    # Ensure the CSV file exists and write headers if it's the first time
    if not os.path.exists(TMP_CSV_PATH):
        with open(TMP_CSV_PATH, mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write the header
            writer.writerow(flattened_data.keys())  # Write headers based on the keys

    # Write the flattened data to CSV
    with open(TMP_CSV_PATH, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(flattened_data.values())  # Write values in the same order as the header

@dag(
    dag_id="kafka_consumer_to_spark_csv",
    start_date=datetime(2023, 4, 1),
    schedule_interval=None,  # Set to None to disable automatic scheduling
    catchup=False,
    tags=["kafka", "consumer", "batch", "csv"],
)
def kafka_consumer():
    # Task: Consume messages from Kafka and write to temporary CSV file
    consume_task = ConsumeFromTopicOperator(
        task_id="consume_from_testdata",
        kafka_config_id="kafka_default_consumer",  # Kafka connection configured in Airflow
        topics=[KAFKA_TOPIC],  # Kafka topic to consume from
        apply_function=consume_function,  # Function to handle messages
        poll_timeout=60,  # Time to wait for messages in seconds
        max_messages=5000,  # Number of messages to process in each batch (adjust as needed)
    )

    # Task: Submit Spark job to read the CSV file and process the data
    spark_submit_task = SparkSubmitOperator(
        task_id="submit_spark_job",
        conn_id="spark_default",  # Spark connection configured in Airflow
        application="/opt/airflow/dags/insert.py",  # Path to your Spark job (Python file)
        name="kafka_to_postgres_batch_job",
        conf={"spark.executor.memory": "8g", "spark.driver.memory": "8g"},
        application_args=[TMP_CSV_PATH],  # Pass the CSV file path to the Spark job
        driver_class_path="/opt/airflow/dags/postgresql-42.7.2.jar",  # Path to PostgreSQL JDBC driver
        jars="/opt/airflow/dags/postgresql-42.7.2.jar",  # Path to PostgreSQL JDBC driver
        deploy_mode="client",  # Deploy mode
    )

    # Task: Delete the temporary CSV file after Spark job is done
    def delete_temp_csv():
        """Deletes the global CSV file after processing."""
        if os.path.exists(TMP_CSV_PATH):
            os.remove(TMP_CSV_PATH)
            print(f"Deleted temporary CSV file: {TMP_CSV_PATH}")

    delete_temp_csv_task = PythonOperator(
        task_id="delete_temp_csv",
        python_callable=delete_temp_csv,
    )

    # Set task dependencies
    consume_task >> spark_submit_task >> delete_temp_csv_task

# Instantiate the DAG
kafka_consumer()
