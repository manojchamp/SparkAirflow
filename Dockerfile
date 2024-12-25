FROM apache/airflow

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get install nano \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

USER airflow

# Install airflow and providers with correct versions
RUN pip install apache-airflow==2.10.4 \
                apache-airflow-providers-apache-kafka \
                
                apache-airflow-providers-apache-spark \
                apache-airflow-providers-openlineage>=1.8.0 \
                pyspark
