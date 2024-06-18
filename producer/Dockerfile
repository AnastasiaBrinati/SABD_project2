FROM python:3.11

# Update packages and install OpenJDK
RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

WORKDIR /home/producer

RUN mkdir config

COPY config/requirements.txt .
COPY config/default.json config

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the application
CMD ["/bin/bash"]