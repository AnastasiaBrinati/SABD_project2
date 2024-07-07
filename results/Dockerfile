FROM python:3.11

RUN apt-get update
RUN apt-get -y install python3-pip

WORKDIR /app/src

COPY config/requirements.txt .
COPY src  ./src

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the application
CMD ["/bin/bash"]