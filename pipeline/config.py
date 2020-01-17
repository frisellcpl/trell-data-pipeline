from typing import Dict
from pathlib import Path

import yaml
from yaml.parser import ParserError

from pipeline.types import T


class ConfigLoadError(Exception):
    pass


def load_config_file(config_file: Path) -> Dict[str, T]:
    """
    Load a YAML config file from the given path.
    Raises:
        ConfigLoadError if config file could not be loaded.
    Returns:
         The parsed config values.
    """

    try:
        file = open(str(config_file), 'r')
        config = yaml.load(file)
        file.close()

        if type(config) is not dict:
            raise TypeError('Config must be a map at root level.')

    except IOError as e:
        raise ConfigLoadError(
            'Failed opening config file: {msg}.'.format(msg=e)
        ) from e

    except (ParserError, TypeError) as e:
        raise ConfigLoadError(
            'Failed parsing config file: {msg}'.format(msg=e)
        ) from e

    else:
        return config
