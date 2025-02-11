import logging
from logging.handlers import TimedRotatingFileHandler
import os

env= os.getenv("logfilename")

# Directory to store log files
log_dir = '../logs'
os.makedirs(log_dir, exist_ok=True)


log_file_path = os.path.join(log_dir, env)


# Configure the logging system with a basic level
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



# Create a TimedRotatingFileHandler for logging to a file with daily rotation
file_handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=7)
file_handler.suffix = "%Y-%m-%d"  # Use date suffix for filenames
file_handler.setLevel(logging.DEBUG)  # Set the logging level for the file handler

# Create a logger object
common_logger = logging.getLogger('saso_logger')

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
common_logger.addHandler(file_handler)

# Optionally, add a stream handler to output logs to the console
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
common_logger.addHandler(stream_handler)
