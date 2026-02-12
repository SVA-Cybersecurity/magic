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
import csv
import hashlib
from glob import glob
from concurrent.futures import ThreadPoolExecutor
from ..interfaces.enricher import BaseEnricher
from ..helpers.utils import TaskWrapper, write_json_to_file
from ..helpers.registry import register_enricher


@register_enricher(name="hash")
class Hash(BaseEnricher):

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self):
        if self._is_enabled():
            self.logger.debug(f"Create output_hash task with params: {self.config.model_dump()}")

            return [TaskWrapper(name="output_hash", coroutine=self.output_hash())]

        return []

    async def output_hash(self):
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as executor:
            output_file = os.path.join(self.output_dir, self.config.output_filename)
            output_file_csv = os.path.join(self.output_dir, self.config.output_filename_csv)
            with open(output_file, "w") as out_file:
                for filename in glob(f"{self.output_dir}/**/*.json", recursive=True):
                    await loop.run_in_executor(executor, self.process_file, filename, out_file)
        self.jsonl_to_csv(jsonl_file=output_file, csv_file=output_file_csv)

    def jsonl_to_csv(self, jsonl_file, csv_file):
        with open(jsonl_file, 'r') as infile, open(csv_file, 'w', newline='') as outfile:
            writer = None

            for line in infile:
                """Parse the JSON from the line"""
                data = json.loads(line)

                """ Initialize the CSV writer with the headers from the first line """
                if writer is None:
                    """Use the keys from the first JSON object as the CSV headers"""
                    headers = data.keys()
                    writer = csv.DictWriter(outfile, fieldnames=headers)
                    writer.writeheader()

                """ Write the JSON data as a CSV row """
                writer.writerow(data)

    def process_file(self, filename, out_file):
        try:
            h = hashlib.sha256()
            b = bytearray(128 * 1024)
            mv = memoryview(b)
            line_count = 0
            previous_ended_with_newline = False

            with open(filename, "rb") as in_file:

                while n := in_file.readinto(mv):
                    """Update hash with the newly read bytes"""
                    data = mv[:n].tobytes()

                    """ Update hash with the newly read bytes """
                    h.update(data)

                    """ Count lines in the current chunk """
                    line_count += data.count(b'\n')

                    """ Check if the buffer ends with a newline """
                    previous_ended_with_newline = data.endswith(b'\n')

            """ Check if the last line didn't end with a newline """
            if not previous_ended_with_newline and line_count > 0:
                line_count += 1

            json_line = {"filename": filename, "sha256": h.hexdigest(), "line_count": line_count}

            write_json_to_file(json_line, out_file)

        except Exception as e:
            self.logger.error(f"Error hashing file {filename}: {e}")
