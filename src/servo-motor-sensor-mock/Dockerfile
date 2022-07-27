FROM python:3.8-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY src ./src/
WORKDIR /app/src

CMD python3 main.py \
    --redpanda-host=$SERVO_MOTOR_SENSOR_MOCK_REDPANDA_HOST \
    --redpanda-port=$SERVO_MOTOR_SENSOR_MOCK_REDPANDA_PORT \
    --redpanda-topic=$SERVO_MOTOR_SENSOR_MOCK_REDPANDA_TOPIC \
    --servo-position=$SERVO_MOTOR_SENSOR_MOCK_SERVO_POSITION \
    --log-level=$SERVO_MOTOR_SENSOR_MOCK_LOG_LEVEL