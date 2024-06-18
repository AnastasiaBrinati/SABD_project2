FROM apache/nifi:latest

# Copy the flow configuration files
COPY --chown=nifi:nifi config/flow.xml.gz $NIFI_HOME/conf/flow.xml.gz
COPY --chown=nifi:nifi config/flow.json.gz $NIFI_HOME/conf/flow.json.gz

EXPOSE 8443
EXPOSE 8081