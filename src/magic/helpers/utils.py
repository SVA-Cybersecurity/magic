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
import sys
import json
import time
import datetime
from uuid import UUID
from dataclasses import dataclass
from typing import Coroutine, List


@dataclass
class TaskWrapper:
    """wrapper class for dedicated task creation and gathering"""

    name: str
    coroutine: Coroutine


def close_coroutines(tasks: List[Coroutine]) -> None:
    """close all coroutine objects | close all TaskWrapper coroutines wrapped in a coroutine"""
    for task in tasks:
        if hasattr(task.cr_frame, "f_locals"):
            task.cr_frame.f_locals['task'].coroutine.close()
        task.close()


def check_output_dir(output_dir, logger):
    if not os.path.exists(output_dir):
        if logger:
            logger.debug(f'Output directory "{output_dir}" does not exist. Attempting to create.')
        try:
            os.makedirs(output_dir)
        except Exception as e:
            if logger:
                logger.error(f"Error while attempting to create output directory {output_dir}: {str(e)}")
            raise
    elif not os.path.isdir(output_dir):
        if logger:
            logger.error(f"{output_dir} exists but is not a directory or you do not have permissions to access. Exiting.")
        sys.exit(1)


def snake_to_camel(snake_str):
    components = snake_str.split("_")
    return components[0].capitalize() + "".join(x.capitalize() for x in components[1:])


def convert_keys_to_camel_case(obj):
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            new_key = snake_to_camel(k)
            new_dict[new_key] = convert_keys_to_camel_case(v)
        return new_dict
    elif isinstance(obj, list):
        return [convert_keys_to_camel_case(i) for i in obj]
    else:
        return obj


async def log_task(task: TaskWrapper, logger, *args, **kwargs):
    seconds = time.perf_counter()
    logger.debug(f"Task '{task.name}' started")
    try:
        await task.coroutine
    finally:
        elapsed = time.perf_counter() - seconds
        logger.info(f"Task '{task.name}' executed in {elapsed:0.2f} seconds")


async def semaphore_wrapper(function, semaphore=None, *args, **kwargs):
    if semaphore:
        async with semaphore:
            await function(*args, **kwargs)
    else:
        await function(*args, **kwargs)


def custom_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, datetime.time):
        return obj.strftime('H:i:s')
    elif isinstance(obj, datetime.timedelta):
        return (datetime.datetime.now() + obj).isoformat()
    elif isinstance(obj, UUID):
        return str(obj)
    elif hasattr(obj, "__dict__"):
        obj_dict = {
            k: v for k, v in obj.__dict__.items() if k != "backing_store" and v not in (None, "none", "None", "", {}, [])
        }
        for key, value in obj_dict.items():
            if isinstance(value, str):
                if value.lower() == "true":
                    obj_dict[key] = True
                elif value.lower() == "false":
                    obj_dict[key] = False
        return convert_keys_to_camel_case(obj_dict)
    elif isinstance(obj, int):
        return str(obj)
    elif isinstance(obj, str):
        return obj
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    else:
        raise TypeError(f"Type {type(obj)} not serializable")


def end_of_day(dt: datetime.datetime) -> datetime.datetime:
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def next_midnight(dt: datetime.datetime) -> datetime.datetime:
    d = dt.date() + datetime.timedelta(days=1)
    return datetime.datetime(d.year, d.month, d.day, tzinfo=dt.tzinfo)


def daterange(start_date: datetime.datetime, end_date: datetime.datetime, number_interval_days: int):

    if number_interval_days < 0:
        raise ValueError("number_interval_days must be non-negative")

    if start_date > end_date:
        return

    if number_interval_days == 0:
        yield start_date, end_date
        return

    current_start = start_date

    while current_start <= end_date:
        if is_midnight(current_start):
            raw_day = current_start + datetime.timedelta(days=number_interval_days - 1)
            interval_end = end_of_day(raw_day)
        else:
            interval_end = current_start + datetime.timedelta(days=number_interval_days)

        if interval_end > end_date:
            interval_end = end_date

        yield current_start, interval_end

        # Stop if we've reached the end_date
        if interval_end >= end_date:
            break

        if is_midnight(current_start):
            next_start = next_midnight(interval_end)
        else:
            next_start = interval_end

        if next_start <= current_start:
            break

        current_start = next_start


def is_midnight(dt):
    """Check if a datetime object represents midnight (00:00:00)."""
    return (dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0) or (
        dt.hour == 23 and dt.minute == 59 and dt.second == 59 and dt.microsecond == 999999
    )


def date_string_in_file_identifier(date_start, date_end):
    """Generate file identifier strings based on start and end datetime objects."""

    start_format = "%Y-%m-%d" if is_midnight(date_start) else "%Y-%m-%dT%H:%M:%S"

    # Determine the format for date strings
    if date_start.replace(hour=0, minute=0, second=0, microsecond=0) == date_end.replace(
        hour=0, minute=0, second=0, microsecond=0
    ):
        # Start and end are one day apart
        end_format = "" if is_midnight(date_start) and is_midnight(date_end) else "%Y-%m-%dT%H:%M:%S"
    else:
        # Start and end are not one day apart
        end_format = "%Y-%m-%d" if is_midnight(date_end) else "%Y-%m-%dT%H:%M:%S"

    return date_start.strftime(start_format), date_end.strftime(end_format)


def remove_odata_fields(json_dict):
    return {k: v for k, v in json_dict.items() if not (k.startswith("@odata") or k.startswith("OdataType"))}


def write_json_to_file(json_input, file, extra_json=None):
    serialized_json = json.dumps(json_input, ensure_ascii=True, default=custom_serializer)

    deserialized_json = json.loads(serialized_json)
    if "AdditionalData" in deserialized_json:
        additional_data = deserialized_json.pop("AdditionalData")
        if isinstance(additional_data, dict):
            deserialized_json.update(additional_data)

    cleaned_json = remove_odata_fields(deserialized_json)

    if extra_json:
        cleaned_json.update(extra_json)
        cleaned_json = remove_odata_fields(cleaned_json)

    final_output = json.dumps(cleaned_json, ensure_ascii=True)

    file.write(f"{final_output}\n")
