# Usa l'immagine base di Apache Flink di Bitnami come punto di partenza
FROM bitnami/flink:latest

# Copia il file di configurazione personalizzato nel container
COPY ./config/flink-conf.yaml /opt/bitnami/flink/conf/config.yaml

# Comando predefinito per avviare il JobManager
CMD ["taskmanager"]
