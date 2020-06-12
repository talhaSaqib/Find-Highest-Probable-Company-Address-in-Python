"""

This file contains a class that configures basic settings needed for every Python program, such as:

> Config Parser
> Logger
> Panda Display
> Warnings on/off
> Plot

"""

__author__ = "Talha Saqib"


class Configurator(object):

    def __init__(self):
        pass

    @staticmethod
    def set_config_parser():
        import configparser as config
        config_parser = config.ConfigParser()
        config_parser.read("config.ini")
        return config_parser

    @staticmethod
    def set_logger():
        import logging
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s-%(levelname)s-%(funcName)s-%(lineno)d-%(message)s')
        logger = logging.getLogger()
        return logger

    @staticmethod
    def set_warnings_off():
        import warnings
        warnings.filterwarnings("ignore")

    @staticmethod
    def set_pandas_display():
        import pandas as pd
        # Overriding output display dimensions
        pd.set_option('display.max_columns', 10)
        pd.set_option('display.max_rows', 7300)

    @staticmethod
    def set_plot():
        import matplotlib.pyplot as plt
        fig_size = plt.rcParams["figure.figsize"]
        fig_size[0] = 6
        fig_size[1] = 6
        plt.rcParams["figure.figsize"] = fig_size





