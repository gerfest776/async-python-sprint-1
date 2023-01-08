import logging

logging.basicConfig(
    filename="app.log",
    filemode="a",
    format="%(name)s::%(levelname)s - %(message)s",
    level=logging.DEBUG,
)


def get_logger():
    return logging.getLogger(__name__)
