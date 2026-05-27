"""
base logger, only a wrapper around the logger
"""
import logging
class ShmModuleBaseLogger:

    """
    log if set, basic api
    """

    def __init__(self,
                 logger: logging.Logger = None):
        """
        default init for logger which other classes inherit.
        basically a wrapper around the logger which prints the log
        only if logger is set

        Parameters
        ----------
        logger : logging.Logger, optional
            logger to be used, by default None
        """
        if logger is not None and not isinstance(logger, logging.Logger):
            raise ValueError(f"logger must be of type logging.Logger, instead got {type(logger)}")
        self._logger = logger

    def info(self, message: str, *args):
        """
        log message info
        """
        if self._logger is not None:
            self._logger.info(message, *args)

    def debug(self, message: str, *args):
        """
        log message debug
        """
        if self._logger is not None:
            self._logger.debug(message, *args)

    def warning(self, message: str, *args):
        """
        log message warning
        """
        if self._logger is not None:
            self._logger.warning(message, *args)

    def error(self, message: str, *args):
        """
        log message error
        """
        if self._logger is not None:
            self._logger.error(message, *args)

    def exception(self, message: str, *args):
        """
        log message exception
        """
        if self._logger is not None:
            self._logger.exception(message, *args)

    def critical(self, message: str, *args):
        """
        log message critical
        """
        if self._logger is not None:
            self._logger.critical(message, *args)
