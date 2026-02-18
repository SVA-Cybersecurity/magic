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
import os
import json
import re
import requests
from ..interfaces.enricher import BaseEnricher
from ..helpers.utils import TaskWrapper, write_json_to_file
from ..helpers.registry import register_enricher


@register_enricher(name="ipapi")
class IpApi(BaseEnricher):

    IP_PATTERNS = [
        "^(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
        "^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$",
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self):
        if self._is_enabled():
            self.logger.debug(f"Create output_ipapi task with params: {self.config.model_dump()}")

            return [TaskWrapper(name="output_ipapi", coroutine=self.output_ipapi())]

        return []

    async def output_ipapi(self):
        # Check if ipapi settings are configured
        if self.settings.ipapi is None:
            self.logger.error("ipapi settings are not configured. Please add an 'ipapi' section to your config file.")
            return

        input_file = os.path.join(self.output_dir, self.config.input_filename)
        output_file = os.path.join(self.output_dir, self.config.output_filename)

        if not os.path.exists(input_file):
            self.logger.error(
                f"File {input_file} not found. Please configure the jsonl enricher before processing data to ipapi enrichment!"
            )
            return

        with open(output_file, "w") as out_file:

            self.logger.debug(f"Converting file {input_file} with ipapi enrich module")

            with open(input_file, "rb") as in_file:
                for line in in_file:
                    try:
                        json_line = json.loads(line)

                        for key, val in json_line.copy().items():
                            for pattern in self.IP_PATTERNS:
                                match = re.match(pattern=pattern, string=val)

                                try:
                                    if match:
                                        try:
                                            payload = {'ips': [match.string], 'key': self.settings.ipapi.key}

                                            res = requests.post(
                                                self.settings.ipapi.endpoint, json=payload, verify=self.settings.ipapi.cert
                                            )
                                            res.raise_for_status()

                                            data = res.json()

                                        except requests.RequestException as req_err:
                                            self.logger.error(f"RequestException at ip enrichment: {req_err}")
                                            continue
                                        except ValueError as val_err:
                                            self.logger.error(f"Invalid JSON returned from ipapi: {val_err}")
                                            continue

                                        value = data.get(match.string, {})
                                        if not isinstance(value, dict):
                                            self.logger.error(
                                                f"Response from ipapi is not of type dict {match.string}: {value}"
                                            )
                                            continue

                                        serialized = {k: str(v) for k, v in value.items()}

                                        self.logger.debug(f"IP information enriched: {repr(serialized)[:40]}...")
                                        json_line[key] = str(serialized)

                                except Exception as e:
                                    self.logger.error(e)
                                    self.logger.error(json_line)

                        write_json_to_file(json_line, out_file)

                    except Exception as e:
                        self.logger.error(e)
