# Dockerfile.consumer

FROM python:3.9-slim

WORKDIR /app

# Install required Python packages
RUN pip install kafka-python

# Copy the consumer script
COPY results_consumer.py .

# Run the consumer script
#CMD ["python", "results_consumer.py"]