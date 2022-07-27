FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY src .
WORKDIR /app/src

CMD python3 main.py \
    --redpanda-host=$DEMO_PRODUCER_REDPANDA_HOST \
    --redpanda-port=$DEMO_PRODUCER_REDPANDA_PORT \
    --redpanda-topic=$DEMO_PRODUCER_REDPANDA_TOPIC \
    --produce-messages=$DEMO_CONSUMER_PRODUCE_MESSAGES \
    --log-level=$DEMO_CONSUMER_LOG_LEVEL