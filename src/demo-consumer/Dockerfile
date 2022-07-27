FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY src .
WORKDIR /app/src

CMD python3 main.py \
    --redpanda-host=$DEMO_CONSUMER_REDPANDA_HOST \
    --redpanda-port=$DEMO_CONSUMER_REDPANDA_PORT \
    --redpanda-topic=$DEMO_CONSUMER_REDPANDA_TOPIC \
    --redpanda-group-id=$DEMO_CONSUMER_REDPANDA_GROUP_ID \
    --log-level=$DEMO_CONSUMER_LOG_LEVEL