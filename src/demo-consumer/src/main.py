import os
import click
import logging

from logging import Logger, getLogger
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord


# It’s essential to set a group ID so that the consumer commits the offset to the Redpanda broker.
# Otherwise, every time you start the worker,
# it will read the message from the start rather than from the last message you were last time.
DEFAULT_REDPANDA_GROUP_ID: str = "group1"
DEFAULT_REDPANDA_HOST: str = "localhost"
DEFAULT_REDPANDA_PORT: str = "9092"
DEFAULT_REDPANDA_TOPIC: str = "demo_topic"
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


def consume_messages(consumer: KafkaConsumer, logger: Logger) -> None:
    """A simple function to consume messages from the
    RedPanda topics. And then print them.

    Args:
        consumer: An instance of the RedPanda consumer.
        logger: A logger instance to log all messages.
    """
    consumer_record: ConsumerRecord  # Type annotation hint

    for consumer_record in consumer:
        message_as_binary: bytes = consumer_record.value
        message: str = message_as_binary.decode("utf-8")

        logger.info(f'Message consumed: "{message}"')


def main(
    redpanda_host: str,
    redpanda_port: str,
    redpanda_topic: str,
    redpanda_group_id: str,
    log_level: int,
) -> None:
    """A simple wrapper to encapsulate all the steps to consume the RedPanda messages
    from a topic.

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
        Nothing
    """
    logger: Logger = get_logger(log_level)
    redpanda_server: str = f"{redpanda_host}:{redpanda_port}"

    logger.info(
        f"Start listening topic: {redpanda_topic}, with group id: {redpanda_group_id}"
    )

    try:
        consumer: KafkaConsumer = KafkaConsumer(
            redpanda_topic,
            bootstrap_servers=[redpanda_server],
            group_id=redpanda_group_id,
        )

    except Exception as e:
        logger.error(
            f"Consumer was not properly initialized, exiting. Original error: {e}"
        )

        exit()

    try:
        consume_messages(consumer, logger)

    except Exception as e:
        logger.error(
            f"Some errors happend while consuming messages. Original error: {e}"
        )

        exit()


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
    """Starts consumer process.

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
        Nothing
    """
    log_level: int = int(log_level)

    main(redpanda_host, redpanda_port, redpanda_topic, redpanda_group_id, log_level)


if __name__ == "__main__":
    cli()
