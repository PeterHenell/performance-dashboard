from config_manager import ConfigManager
from stats_collector import StatCollector
import sys
import logging

__author__ = "Peter Henell"
__copyright__ = "Copyright 2016, Peter Henell"
__credits__ = ["Peter Henell"]
__license__ = "Apache License 2.0"
__version__ = "1.0.0"
__maintainer__ = "Peter Henell"
__email__ = "dnd"
__status__ = "dev"


logger = logging.getLogger('perf-collector')
handler = logging.StreamHandler()
logger.addHandler(handler)


def main(arguments):
    import os.path

    global application_config
    if len(arguments) == 0:
        print('Usage: main.py <settings_file.ini> [truncate_data]')
        print('Specify truncate_data if you wish to clear all the data')
        print()
        print('Example usage: main.py localhost.ini')
        print('Example usage: main.py localhost.ini truncate_data')
        exit(2)

    settings_file = arguments[0]

    if not os.path.isfile(settings_file):
        print('Settings file not found: %s' % settings_file)

    config_manager = ConfigManager.from_file(settings_file)
    config_manager.set_logging()
    application_config = config_manager.get_config('performance-collector')

    stats_collector = StatCollector(config_manager)

    if len(arguments) == 2:
        if arguments[1] == 'truncate_data':
            stats_collector.cleanup()

    stats_collector.run()


if __name__ == '__main__':
    main(sys.argv[1:])