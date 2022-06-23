from logging import getLogger, Logger, config

_defaults_logging_config = {
    'version':1,
    'formatters': {
        'simple': {
            'format':'%(asctime)s-%(name)s:%(lineno)d-%(levelname)s: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        }
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console']
    }
}

config.dictConfig(_defaults_logging_config)

def get_logger(name:str) -> Logger:
    return getLogger(name)
