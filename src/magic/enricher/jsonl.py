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
import os
import json
from glob import glob
from concurrent.futures import ThreadPoolExecutor
from ..interfaces.enricher import BaseEnricher
from ..helpers.utils import TaskWrapper, write_json_to_file
from ..helpers.registry import register_enricher


@register_enricher(name="jsonl")
class Jsonl(BaseEnricher):

    OUTPUT_FILENAME: str = "base.jsonl"

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self):
        self.logger.debug("Create output_jsonl Task")

        return [TaskWrapper(name="output_jsonl", coroutine=self.output_jsonl())]

    async def output_jsonl(self):
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as executor:
            output_file = os.path.join(self.output_dir, self.OUTPUT_FILENAME)
            with open(output_file, "w") as out_file:
                for file in glob(f"{self.output_dir}/**/*.json", recursive=True):
                    self.logger.debug(f"Converting file {file} with jsonl enrich module")
                    await loop.run_in_executor(executor, self.process_file, file, out_file)

    def process_file(self, file, out_file):
        with open(file, "rb") as in_file:
            for line in in_file:
                try:
                    json_line = json.loads(line)

                    if isinstance(json_line, dict):
                        """Convert all values to strings"""
                        for key, val in json_line.items():
                            json_line[key] = str(val)

                        json_line["filename"] = str(os.path.basename(file))
                        json_line["path"] = str(file)
                        json_line["message"] = ""

                        write_json_to_file(json_line, out_file)
                    else:
                        raise
                except Exception:
                    self.logger.warning(f"Invalid JSON line in file {file}")
