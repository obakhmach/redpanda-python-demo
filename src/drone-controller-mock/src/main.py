import json
import logging
import random
import time
import uuid
from functools import wraps
from datetime import datetime
from logging import Logger, getLogger
from typing import Dict, Callable, Any

import click
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

DEFAULT_REDPANDA_HOST: str = "localhost"
DEFAULT_REDPANDA_PORT: str = "9092"
DEFAULT_REDPANDA_TOPIC: str = "servo_sys_info"
DEFAULT_MESSAGES_PRODUCE = 15
DEFAULT_LOGGING_FORMAT: str = "[%(asctime)s][%(levelname)s] %(message)s"

MOCK_SERVO_ID: str = str(uuid.uuid4())


def get_logger(log_level: int) -> Logger:
    """The simple intanciator and getter for logger
    Args:
        log_level: Logging level as an integer. CRITICAL: 50, ERROR: 40, WARNING: 30, INFO: 20, DEBUG: 10, NOTSET: 0.
    Returns:
        Logger instance.
    """
    logging.basicConfig(format=DEFAULT_LOGGING_FORMAT)

    logger: Logger = getLogger(__name__)
    logger.setLevel(log_level)

    return logger


def execute_with_retries(
    retries: int, retries_timeout: int, logger: Logger
) -> Callable:
    """A helper decorator to wrap some execution with retry logic
    """
    def decorator(f: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            nonlocal retries
            nonlocal retries_timeout
            nonlocal logger

            while retries >= 1:
                try:
                    return f(*args, **kwargs)

                except Exception as e:
                    message: str = f"Received exception {e} on retry."

                    logger.warning(message)

                time.sleep(retries_timeout)
                retries -= 1

            return f(*args, **kwargs)

        return wrapper

    return decorator


def mock_servo_sys_information() -> Dict:
    mock_id: str = MOCK_SERVO_ID
    mock_datetime: str = str(datetime.now())
    mock_iterruption_count: int = random.randint(0, 10)
    mock_average_angle_speed: float = random.uniform(50, 75)
    mock_average_angle_momentum: float = random.uniform(1, 2)

    return {
        "id": mock_id,
        "datetime": mock_datetime,
        "iterruptionCount": mock_iterruption_count,
        "averageAngleSpeed": mock_average_angle_speed,
        "averageAngleMomentum": mock_average_angle_momentum,
    }


def create_topic(
    name: str, host: str, num_partitions: int = None, replication_factor: int = None
):
    """The simple function wraps the steps needed to create
    a new topic inside RedPanda with some extra parameters passed.

    Args:
        name: A name of the topic.
        host: A host of redpanda backend within the topic will be created.
        num_partitions: Set the number of partitions for the new topic.
        replication_factor: Set the replication fuctor for the new topic.

    Returns:
        Nothing.
    """
    # Set the default number of partitions.
    if num_partitions is None:
        num_partitions = 1

    # Set the default number of replication factor
    if replication_factor is None:
        replication_factor = 1

    # Create a new topic instance
    topic = NewTopic(
        name=name, num_partitions=num_partitions, replication_factor=replication_factor
    )

    # Instancialte a new client to send requests to RedPanda
    admin = KafkaAdminClient(bootstrap_servers=host)
    # Within instanciated client we create a new topic.
    admin.create_topics([topic])


def mock_servo_sys_info_producer(
    producer: KafkaProducer, topic_name: str, servo_position: str, logger: Logger
):
    """The mock function mimics the behavior of the drone servo and
    dispatches the fake messages about
    all the system information received from the servo senors.

    Args:
        procuder: Initialized RedPanda producer.
        topic_name: The name of the topic where we want to dispatch messages.
        servo_position: The position of the servo on the drone.
        logger: The logger instance to log ingormation.

    Returns:
        Nothing.
    """
    system_delay_for_mock_messages_readability: float = 3

    # Loop trough nessages and send them
    while True:
        mocked_servo_sys_information: Dict = mock_servo_sys_information()

        logger.info(
            'SysInfo for {} id -> "{}"'.format(
                servo_position, mocked_servo_sys_information.get("id")
            )
        )
        logger.info(
            '\t| Datetime -- "{}"'.format(mocked_servo_sys_information.get("datetime"))
        )
        logger.info(
            '\t| Iterruption count -- "{}"'.format(
                mocked_servo_sys_information.get("iterruptionCount")
            )
        )
        logger.info(
            '\t| Average angle speed -- "{}"'.format(
                mocked_servo_sys_information.get("averageAngleSpeed")
            )
        )
        logger.info(
            '\t| Average angle momentum -- "{}"'.format(
                mocked_servo_sys_information.get("averageAngleMomentum")
            )
        )

        mocked_servo_sys_information_serialized: bytes = json.dumps(
            mocked_servo_sys_information
        ).encode("utf-8")

        try:
            producer.send(topic_name, mocked_servo_sys_information_serialized)

        except Exception as e:
            logger.info(
                f'Sending sys info to "{topic_name}" was failed' f"Original error: {e}"
            )

        logger.info(f'Sys info was sent  to "{topic_name}"')

        time.sleep(system_delay_for_mock_messages_readability)


def main(
    redpanda_host: str,
    redpanda_port: str,
    redpanda_topic: str,
    servo_poistion: int,
    log_level: int,
) -> None:
    """A wrapper to correctly produce messages

    Args:
        redpanda_host: The host of the running redpanda server.
        redpanda_port: TThe port of the running redpanda server.
        redpanda_topic: The topic of the running redpanda server
                        where producer will be send messages.
        servo_position: The position of the servo on the drone.
        log_level: Logging level as an integer. CRITICAL: 50, ERROR: 40,
                   WARNING: 30, INFO: 20, DEBUG: 10, NOTSET: 0.

    Returns:
        Nothing.
    """
    logger: Logger = get_logger(log_level)

    logger.debug(
        f"Configurations -> RedPanda host: {redpanda_host}, "
        f"RedPanda port: {redpanda_port}, RedPanda topic: {redpanda_topic}"
    )

    redpanda_server: str = f"{redpanda_host}:{redpanda_port}"

    retries: int = 10
    retries_timeout = 3

    producer: KafkaProducer = execute_with_retries(retries, retries_timeout, logger)(
        KafkaProducer
    )(bootstrap_servers=[redpanda_server])

    try:
        logger.info(f"Creating a topic: {redpanda_topic}")

        create_topic(redpanda_topic, redpanda_server)

        logger.info(f"Topic {redpanda_topic} was created.")

    except TopicAlreadyExistsError:
        logger.warning(f"Topic: {redpanda_topic} already exists")

    logger.info(f"Start producing messages to {redpanda_topic}")

    mock_servo_sys_info_producer(producer, redpanda_topic, servo_poistion, logger)


@click.command()
@click.option(
    "-h",
    "--redpanda-host",
    "redpanda_host",
    required=True,
    default=DEFAULT_REDPANDA_HOST,
    help="The host of the running redpanda server.",
    type=click.STRING,
)
@click.option(
    "-p",
    "--redpanda-port",
    "redpanda_port",
    required=True,
    default=DEFAULT_REDPANDA_PORT,
    help="The port of the running redpanda server.",
    type=click.STRING,
)
@click.option(
    "-t",
    "--redpanda-topic",
    "redpanda_topic",
    required=True,
    default=DEFAULT_REDPANDA_TOPIC,
    help="The topic of the running redpanda server "
    "where producer will be send messages.",
    type=click.STRING,
)
@click.option(
    "-g",
    "--redpanda-group-id",
    "redpanda_group_id",
    required=True,
    default=DEFAULT_REDPANDA_TOPIC,
    help="TIt’s essential to set a group ID so that the consumer commits the offset"
    "to the Redpanda broker."
    "Otherwise, every time you start the worker,"
    "it will read the message from the start rather than from the last message"
    "you were last time.",
    type=click.STRING,
)
@click.option(
    "-l",
    "--log-level",
    "log_level",
    required=True,
    help="Logging level as an integer. CRITICAL: 50, ERROR: 40, "
    "WARNING: 30, INFO: 20, DEBUG: 10, NOTSET: 0.",
    type=click.Choice(
        [
            str(logging.CRITICAL),
            str(logging.ERROR),
            str(logging.WARNING),
            str(logging.INFO),
            str(logging.DEBUG),
            str(logging.NOTSET),
        ]
    ),
    default=str(logging.INFO),
)
def cli(
    redpanda_host: str,
    redpanda_port: str,
    redpanda_topic: str,
    redpanda_group_id: str,
    log_level: str,
) -> None:
    """The simple function to start the producer process.

    Args:
        redpanda_host: The host of the running redpanda server.
        redpanda_port: TThe port of the running redpanda server.
        redpanda_topic: The topic of the running redpanda server
                        where producer will be send messages.
        redpanda_group_id: It’s essential to set a group ID so that the consumer commits the offset
                           to the Redpanda broker.
                           Otherwise, every time you start the worker,
                           it will read the message from the start rather than from the last message
                           you were last time.
        log_level: Logging level as an integer. CRITICAL: 50, ERROR: 40,
                   WARNING: 30, INFO: 20, DEBUG: 10, NOTSET: 0.

    Returns:
        Nothing.
    """
    log_level = int(log_level)

    main(redpanda_host, redpanda_port, redpanda_topic, redpanda_group_id, log_level)


if __name__ == "__main__":
    cli()
