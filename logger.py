import logging

def setup_logger(log_file='data_transfer.log'):
    logger = logging.getLogger("DataTransfer")
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(file_handler)

    return logger
