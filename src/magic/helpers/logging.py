#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -------------------------------------------------- #
# METADATA                                           #
# -------------------------------------------------- #
__author__ = "Alexander Goedeke"
__version__ = "0.5.0"


# -------------------------------------------------- #
# IMPORTS                                            #
# -------------------------------------------------- #
import logging
import os
from logging import Formatter, DEBUG, INFO


class Logger:

    DEBUG_FORMATTER: Formatter
    INFO_FORMATTER: Formatter

    def __init__(self, name: str, reports_dir: str, debug: bool):

        self.name = name
        self.reports_dir = reports_dir
        self.debug = debug

        self.DEBUG_FORMATTER = Formatter(f"%(asctime)s - %(levelname)s - {name} - %(message)s (%(filename)s:%(lineno)d)")
        self.INFO_FORMATTER = Formatter(f"%(asctime)s - %(levelname)s - {name} - %(message)s")

        return super().__init__()

    def get_formatter(self):
        return self.DEBUG_FORMATTER if self.debug is True else self.INFO_FORMATTER

    def bootstrap(self):
        """Create a logger object"""
        logger = logging.getLogger(self.name)

        """ Prevent messages from being propagated to the root logger """
        logger.propagate = False
        logger.setLevel(DEBUG if self.debug is True else INFO)

        file_handler = logging.FileHandler(os.path.join(self.reports_dir, "magic.log"))
        stream_handler = logging.StreamHandler()

        """ Set the formatter for the handlers """
        file_handler.setFormatter(self.get_formatter())
        stream_handler.setFormatter(self.get_formatter())

        """ Add handlers to the logger """
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        return logger
