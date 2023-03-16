from loguru import logger

format = "[{time:DD.MM.YY HH:mm:ss}] [{module}.{function}:{line}] [{level}]: {message}"
logger.add("debug.log", enqueue=True, format=format, rotation="5 MB", compression="zip", level="DEBUG")