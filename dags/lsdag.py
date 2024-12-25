from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json

# Kafka topic and configuration
KAFKA_TOPIC = "if_admin_status_info"

def consume_function(message):
    """Consumes messages from the Kafka topic and handles decoding errors."""
    try:
        # Attempt to decode the key if it exists
        raw_key = message.key()
        key = json.loads(raw_key) if raw_key else None
    except json.JSONDecodeError as e:
        key = raw_key  # Use raw key if decoding fails
        print(f"Error decoding key: {e}. Raw key: {raw_key}")

    try:
        # Attempt to decode the value
        raw_value = message.value()
        value = json.loads(raw_value)
    except json.JSONDecodeError as e:
        value = raw_value  # Use raw value if decoding fails
        print(f"Error decoding value: {e}. Raw value: {raw_value}")

    print(f"Received Message: Key={key}, Value={value}")

@dag(
    dag_id="kafka_continuous_consumer",  # Unique DAG ID
    start_date=datetime(2023, 4, 1),
    schedule_interval=timedelta(minutes=1),  # Run every 1 minute
    catchup=False,
    tags=["kafka", "consumer", "batch", "continuous"],
)
def kafka_consumer():
    # Task: Continuously consume messages from Kafka in batches
    ConsumeFromTopicOperator(
        task_id="consume_from_testdata",
        kafka_config_id="kafka_default_consumer",  # Kafka connection configured in Airflow
        topics=[KAFKA_TOPIC],  # Kafka topic to consume from
        apply_function=consume_function,  # Function to handle messages
        poll_timeout=60,  # Time to wait for messages in seconds
        max_messages=5000,  # Number of messages to process in each batch (adjust as needed)
        # enable_auto_commit=True,  # Automatically commit offsets
    )

# Instantiate the DAG
kafka_consumer()
