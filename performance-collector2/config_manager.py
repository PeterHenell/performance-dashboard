import configparser
import importlib
import json
import optparse

import logging

from functools import reduce

import sys


class Configuration:

    def __init__(self, config):
        self.config_dict = config

    # Allow access to the hidden dictionary in dictionary style config['key']
    def __getitem__(self, key):
        return self.config_dict[key]


class ClassLoader:
    @staticmethod
    def get_class_from_text(description, full_name):
        # print('Loading %s as %s' % (description, full_name))
        module_name, class_name = full_name.split('.')

        m = importlib.import_module(module_name)
        # get the class, will raise AttributeError if class cannot be found
        c = getattr(m, class_name)
        return c


class CollectorType:
    def __init__(self, class_name, cls):
        self.name = class_name
        self.col_type = cls


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

    def get_available_source_types(self):
        """
            Get list of (collector_name, collector_class) tuples from AvailableCollectors in the config
        """
        collector_types = self.config.items('SourceTypes')
        types = [(key, ClassLoader.get_class_from_text(key, path)) for (key, path) in collector_types]
        collector_types = [CollectorType(n, t) for (n, t) in types]

        return collector_types

    def get_config_for_source_name(self, collector_type_name):
        sources = self.config.items(collector_type_name + '.Sources')
        res = [(key, path) for (key, path) in sources]
        configs = [self.get_config(p) for (k, p) in res]
        return configs

