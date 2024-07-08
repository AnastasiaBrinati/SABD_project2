# Usa l'immagine base di Apache Flink di Bitnami come punto di partenza
FROM flink:latest

# python configuration
RUN apt-get update -y
RUN apt install python3 -y
RUN apt-get update -y
RUN apt-get install python3-pip -y
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install dependencies
RUN pip3 install apache-flink

# Download the Flink Kafka connector JAR along with all necessary dependencies
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

RUN pip3 install jproperties
RUN pip3 install psquare
RUN pip3 install tdigest

COPY src  ./src
COPY src/queries ./queries
COPY src/queries/queries.py .

# Esponi le porte necessarie per il JobManager
EXPOSE 6123 8081

# Comando predefinito per avviare il JobManager
CMD ["jobmanager"]