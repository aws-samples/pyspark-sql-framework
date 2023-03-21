import logging
import logging.config
from config.logging_config_dict import logging_config, DETAILED_FORMATTER, STAT_LEVEL


logging.addLevelName(STAT_LEVEL, "STAT")


class CustomLogger():
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            print('Creating the object')
            cls._instance = super(CustomLogger, cls).__new__(cls)
        return cls._instance


    def set_logger(self):
        logging.addLevelName(STAT_LEVEL, "STAT")
        logging.config.dictConfig(logging_config)
        self.log = logging.getLogger("highlevelLogger")
        return self

    def set_adapter(self, extras: dict, formatter=None):
        logging.config.dictConfig(logging_config)
        self.log.info("Setting the custom logger using the formatter")
        self.log.info(formatter)
        self.custom_log = logging.getLogger('etlLogger')
        handler = self.custom_log.handlers[0]
        handler.setFormatter(fmt=logging.Formatter(formatter if formatter else DETAILED_FORMATTER))
        self.adapter = logging.LoggerAdapter(self.custom_log, extras)
        self.log.info("Formatter {}".format(self.adapter.logger.handlers[0].formatter))
        return self
