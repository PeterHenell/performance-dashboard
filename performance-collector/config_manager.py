import configparser
import importlib
import json
import optparse

import logging

from functools import reduce

import sys


class Configuration:

    def __init__(self, config):
        self.config = config

    # Allow access to the hidden dictionary in dictionary style config['key']
    def __getitem__(self, key):
        return self.config[key]


class ClassLoader:
    @staticmethod
    def get_class_from_text(description, full_name):
        print('Loading %s as %s' % (description, full_name))
        module_name, class_name = full_name.split('.')

        m = importlib.import_module(module_name)
        # get the class, will raise AttributeError if class cannot be found
        c = getattr(m, class_name)
        return c


class ConfigManager:

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger('ConfigManager')
        handler = logging.StreamHandler()
        self.logger.addHandler(handler)

    @staticmethod
    def from_file(file_name):
        parser = optparse.OptionParser()

        parser.add_option('--config', dest='config', default=file_name)
        options, args = parser.parse_args()

        config = configparser.RawConfigParser()
        config.optionxform = str
        config.read(options.config)
        return ConfigManager(config)

    def get_config(self, key):
        return Configuration(dict(self.config.items(key)))

    def set_logging(self):
        logging_conf = self.get_config('logging')
        formatter = logging.Formatter('%(asctime)s - %(name)s.%(funcName)s - %(levelname)s - %(message)s')
        for h in self.logger.handlers:
            h.setFormatter(formatter)
        self.logger.setLevel(logging_conf['log_level'])

    def get_target_databases(self):
        """
        returns a list of tuples of [(databaseName, configKey), ...]
        """
        targets = self.config.items('Target.Databases')
        res = [(key, path) for (key, path) in targets]
        return res

    def get_available_collectors(self):
        """
            Get list of (collector_name, collector_class) tuples from AvailableCollectors in the config
        """
        collector_types = self.config.items('AvailableCollectors')
        types = [(key, ClassLoader.get_class_from_text(key, path)) for (key, path) in collector_types]
        return types

    def get_sources_for_collector(self, collector_type_name):
        sources = self.config.items(collector_type_name + '.Sources')
        res = [(key, path) for (key, path) in sources]
        return res

