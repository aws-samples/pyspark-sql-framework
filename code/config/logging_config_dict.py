DETAILED_FORMATTER = 'service:airflow|step_id:%(step_id)s|msgtype:%(levelname)s|dag_id:%(dag_id)s|run_id:%(run_id)s|process:etl|msg:%(message)s'
DEFAULT_FORMATTER = '%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - %(message)s'
STAT_LEVEL = 60

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'logFormatter': {
            'format': DEFAULT_FORMATTER
        },
        'logDetailedFormatter': {
            'format': DETAILED_FORMATTER
        },
    },
    'handlers': {
        'consoleHandler': {
            'level': 'INFO',
            'formatter': 'logFormatter',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',  # Default is stderr
        },
        'detailedConsoleHandler': {
            'level': 'INFO',
            'formatter': 'logDetailedFormatter',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',  # Default is stderr
        },
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['consoleHandler'],
            'level': 'WARNING',
            'propagate': False
        },
        'etlLogger': {
            'handlers': ['detailedConsoleHandler'],
            'level': 'INFO',
            'propagate': False
        },
        'highlevelLogger': {
            'handlers': ['consoleHandler'],
            'level': 'INFO',
            'propagate': False
        },
    }
}
