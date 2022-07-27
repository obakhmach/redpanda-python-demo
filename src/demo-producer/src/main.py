import logging
import time
import uuid
from logging import Logger, getLogger
from typing import List

import click
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

DEFAULT_REDPANDA_HOST: str = "localhost"
DEFAULT_REDPANDA_PORT: str = "9092"
DEFAULT_REDPANDA_TOPIC: str = "demo_topic"
DEFAULT_MESSAGES_PRODUCE = 15
DEFAULT_LOGGING_FORMAT: str = "[%(asctime)s][%(levelname)s] %(message)s"


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


def produce_messages_with_delay(
    producer: KafkaProducer,
    topic_name: str,
    messages: List[str],
    logger: Logger,
    delay: float = None,
):
    """The simple function to send the given amount of messages to the given topic via
    a given RedPanda Producer delaying on each request  the given delat anout of seconds.

    Args:
        procuder: Initialized RedPanda producer.
        topic_name: The name of the topic where we want to dispatch messages.
        messages: The list of random messages to dispatch.
        delay: The time of the delay in seconds between messages.

    Returns:
        Nothing.
    """
    # Set the default delay value
    if delay is None:
        delay = 0.1

    # Loop trough nessages and send them
    for message in messages:
        logger.info(f'Sending message "{message}" to "{topic_name}"')

        message_as_binary: bytes = message.encode("utf-8")

        try:
            producer.send(topic_name, message_as_binary)

        except Exception as e:
            logger.info(
                f'Sending message "{message}" to "{topic_name}" as failed'
                f"Original error: {e}"
            )

        logger.info(f'Message was sent "{message}" to "{topic_name}"')

        time.sleep(delay)


def main(
    redpanda_host: str,
    redpanda_port: str,
    redpanda_topic: str,
    produce_massages: int,
    log_level: int,
) -> None:
    """A wrapper to correctly produce messages

    Args:
        redpanda_host: The host of the running redpanda server.
        redpanda_port: TThe port of the running redpanda server.
        redpanda_topic: The topic of the running redpanda server
                        where producer will be send messages.
        produce_massages: The number of messages which will be generated
                          by the single process of this producer.
        log_level: Logging level as an integer. CRITICAL: 50, ERROR: 40,
                   WARNING: 30, INFO: 20, DEBUG: 10, NOTSET: 0.

    Returns:
        Nothing.
    """
    logger: Logger = get_logger(log_level)
    redpanda_server: str = f"{redpanda_host}:{redpanda_port}"
    producer: KafkaProducer = KafkaProducer(bootstrap_servers=[redpanda_server])
    delay: float = 2
    messages: List[str] = [
        f"{uuid.uuid4()} created # {i}" for i in range(produce_massages)
    ]

    try:
        logger.info(f"Creating a topic: {redpanda_topic}")

        create_topic(redpanda_topic, redpanda_server)

        logger.info(f"Topic {redpanda_topic} was created.")

    except TopicAlreadyExistsError:
        logger.warning(f"Topic: {redpanda_topic} already exists")

    logger.info(f"Start producing messages to {redpanda_topic}")

    produce_messages_with_delay(producer, redpanda_topic, messages, logger, delay)

    logger.info(f'All messages were sent to "{redpanda_topic}".')
    logger.info("Exiting")


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
    "-m",
    "--produce-messages",
    "produce_massages",
    required=True,
    default=DEFAULT_MESSAGES_PRODUCE,
    help="The number of messages which will be generated "
    "by the single process of this producer.",
    type=click.INT,
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
    produce_massages: int,
    log_level: str,
) -> None:
    """The simple function to start the producer process.

    Args:
        redpanda_host: The host of the running redpanda server.
        redpanda_port: TThe port of the running redpanda server.
        redpanda_topic: The topic of the running redpanda server
                        where producer will be send messages.
        produce_massages: The number of messages which will be generated
                          by the single process of this producer.
        log_level: Logging level as an integer. CRITICAL: 50, ERROR: 40,
                   WARNING: 30, INFO: 20, DEBUG: 10, NOTSET: 0.

    Returns:
        Nothing.
    """
    log_level = int(log_level)

    main(redpanda_host, redpanda_port, redpanda_topic, produce_massages, log_level)


if __name__ == "__main__":
    cli()
