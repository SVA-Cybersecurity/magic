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
import asyncio
from abc import ABC, abstractmethod
from typing import Any, List
from pydantic import BaseModel
from ..helpers.logging import Logger
from ..helpers.utils import check_output_dir, log_task


class IEnricher(ABC):
    @abstractmethod
    def get_tasks(self, data: List[Any]):
        pass

    @abstractmethod
    async def run(self):
        pass


class BaseEnricher(IEnricher):
    def __init__(self, reports_dir, settings, output_dir, config: BaseModel = None, debug: bool = False, logger=__name__):
        logger = Logger(logger, reports_dir, debug)

        self.settings = settings
        self.output_dir = output_dir
        self.config = config or {}
        self.debug = debug
        self.reports_dir = reports_dir
        self.logger = logger.bootstrap()

        check_output_dir(output_dir, self.logger)

    def _is_enabled(self):
        return self.config.enabled is True

    async def run(self):
        tasks = [log_task(task, self.logger) for task in self.get_tasks()]

        if not tasks:
            return

        await asyncio.gather(*tasks)
