# Usa l'immagine base di Apache Flink di Bitnami come punto di partenza
FROM bitnami/flink:latest

# Copia il file di configurazione personalizzato nel container
COPY ./config/flink-conf.yaml /opt/bitnami/flink/conf/config.yaml

# Esponi le porte necessarie per il JobManager
EXPOSE 6123 8081

# Comando predefinito per avviare il JobManager
CMD ["jobmanager"]
