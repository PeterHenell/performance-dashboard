import time

import sys

from config_manager import ConfigManager
from stat_manager import StatManager

__author__ = "Peter Henell"
__copyright__ = "Copyright 2016, Peter Henell"
__credits__ = ["Peter Henell"]
__license__ = "Apache License 2.0"
__version__ = "1.0.0"
__maintainer__ = "Peter Henell"
__email__ = "dnd"
__status__ = "dev"


def main(arguments):
    import os.path

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

    # for stat_collector in stat_collectors:
    if len(arguments) == 2:
        if arguments[1] == 'truncate_data':
            print('Truncating all data')
            # stat_collector.cleanup(config_manager)

    stat_manager = StatManager(config_manager)
    stat_manager.run()


if __name__ == '__main__':
    main(sys.argv[1:])