import os
import configparser

class ConfigParser():

    def __init__(self):
            """
            initialize the file parser with
            ExtendedInterpolation to use ${Section:option} format
            [Section]
            option=variable
            """
            self.config_parser = configparser.ConfigParser()

    def read_ini_file(self, file='C:/Users/harii/_workarea_/msse4/thirdeye/src/config/config.ini'):
        """
        Parses in the passed in INI file and converts any Windows environ vars.

        :param file: INI file to parse
        :return: void
        """
        # Expands Windows environment variable paths
        with open(file, 'r') as cfg_file:
            cfg_txt = os.path.expandvars(cfg_file.read())

        # Parses the expanded config string
        self.config_parser.read_string(cfg_txt)

    def getKey(self, section, option):
        """
        Get an option value for the named section.

        :param section: INI section
        :param option: option tag for desired value
        :return: Value of option tag
        """
        return self.config_parser.get(section, option)


p1 = ConfigParser()
