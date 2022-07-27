FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY src ./src/
WORKDIR /app/src

CMD python3 main.py \
    --redpanda-host=$DRONE_CONTROLLER_MOCK_REDPANDA_HOST \
    --redpanda-port=$DRONE_CONTROLLER_MOCK_REDPANDA_PORT \
    --redpanda-topic=$DRONE_CONTROLLER_MOCK_REDPANDA_TOPIC \
    --redpanda-group-id=$DRONE_CONTROLLER_MOCK_REDPANDA_GROUP_ID \
    --log-level=$SERVO_MOTOR_SENSOR_MOCK_LOG_LEVEL