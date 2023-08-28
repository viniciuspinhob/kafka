import logging
import logging.config

logging.config.fileConfig('util/logging.conf')

# debug < info < warning < error < critical

def log_d(module, message):
    """
    This method logs all debug messages
    """
    logger = logging.getLogger('general')
    logger.debug(module + ' - ' + str(message))


def log_i(module, message):
    """
    This method logs all info messages
    """
    logger = logging.getLogger('general')
    logger.info(module + ' - ' + str(message))
    logging.getLogger('apscheduler.executors.default').propagate = False
    # logging.getLogger('asyncua').propagate = False


def log_w(module, message):
    """
    This method logs all warning messages
    """
    logger = logging.getLogger('general')
    logger.warning(module + ' - ' + str(message))


def log_e(module, message):
    """
    This method logs all error messages
    """
    logger = logging.getLogger('general')
    logger.error(module + ' - ' + str(message))

def log_ex(module, message):
    """
    This method logs all exception messages
    """
    logger = logging.getLogger('general')
    logger.exception(module + ' - ' + str(message))

def log_c(module, message):
    """
    This method logs all critical messages
    """
    logger = logging.getLogger('general')
    logger.critical(module + ' - ' + str(message))