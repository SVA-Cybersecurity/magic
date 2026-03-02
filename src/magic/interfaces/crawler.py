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
import datetime
import asyncio
from abc import ABC, abstractmethod
from typing import List, Awaitable, Optional
from ..helpers.utils import write_json_to_file, date_string_in_file_identifier, daterange, semaphore_wrapper, check_output_dir
from msgraph_beta import GraphServiceClient
from kiota_abstractions.base_request_configuration import RequestConfiguration
from msgraph_beta.generated.models.o_data_errors.o_data_error import ODataError
from json.decoder import JSONDecodeError
from msgraph_beta.generated.models.entity import Entity
from pydantic import BaseModel, EmailStr
from uuid import uuid4, UUID
from ..helpers.logging import Logger
from ..helpers.config import RETENTION_DEFAULT
from ..helpers.mixins import CreateGraphClientMixin


class ICrawler(ABC):
    @abstractmethod
    def get_tasks(self) -> List[Awaitable]:
        pass


class BaseCrawler(ICrawler, CreateGraphClientMixin):

    OUTPUT_FILE_SUFFIX = ".json"

    uuid: UUID

    graph_client: GraphServiceClient | None = None

    DEFAULT_SCOPES: List[str] = ["https://graph.microsoft.com/.default"]

    RETENTION = RETENTION_DEFAULT

    def __init__(self, reports_dir, settings, output_dir, config: BaseModel = None, debug: bool = False, logger=__name__):
        self.uuid = uuid4()
        self.settings = settings
        self.output_dir = output_dir
        self.config = config or {}
        self.debug = debug
        self.reports_dir = reports_dir
        self.logger = Logger(f"{logger}.{self.uuid}", reports_dir, debug).bootstrap()
        self.current_run_permissions = set()

        check_output_dir(output_dir, self.logger)

    async def ensure_graph_client(self, scopes: Optional[List[str]] = None) -> GraphServiceClient | None:
        """Ensure `self.graph_client` is initialized before use.

        This is required because many crawlers build request functions like
        `self.graph_client.users.by_user_id(...).get` *before* passing them into
        `make_graph_request_with_retry`, so `self.graph_client` must exist first.
        """

        # Return existing client if already initialized
        if self.graph_client is not None:
            return self.graph_client

        if scopes is None:
            scopes = self.DEFAULT_SCOPES

        # Create new client without caching
        self.graph_client = await self._create_graph_client(self.settings.auth, scopes=scopes)
        return self.graph_client

    def build_odata_filter(self, **filters) -> str:
        """
        Always includes date_start and date_end.
        Additional filters are added only if they have a non-empty value.
        """

        base_filters = [
            "{filter_timstamp_name} ge {date_start}",
            "{filter_timstamp_name} le {date_end}",
        ]

        # Loop through all provided filters
        for key, value in filters.items():
            if value:  # skip empty values
                base_filters.append(f"{key} eq '{value}'")

        return " and ".join(base_filters)

    def register_required_permissions(self, permissions: List[str]):
        self.current_run_permissions.update(permissions)

    def get_collected_permissions(self) -> List[str]:
        return list(self.current_run_permissions)

    def _get_user_principle_names(self) -> List[Optional[EmailStr]]:
        """getter hierarchy: 1. crawl config -> 2. default config"""
        user_principle_names = getattr(self.config, "user_principle_names", [])

        if not user_principle_names:
            user_principle_names = getattr(self.settings.defaults, "user_principle_names", [])

        return user_principle_names

    def _check_output_file_exists(self, path: str) -> bool:
        if os.path.exists(path):
            self.logger.info(f"Output file '{path}' already exists. Skipping.")
            return True

        return False

    def _read_date_fields(self) -> tuple:
        now = datetime.datetime.now()

        """ date range hierarchy crawl config -> defaults from settings -> default retention"""
        date_start = getattr(self.config, "date_start", None)
        date_end = getattr(self.config, "date_end", None)

        """ fallback if no dates are set at all - use default retention of class implementation """
        if not date_start:
            date_start = (now - datetime.timedelta(days=self.RETENTION)).replace(hour=0, minute=0, second=0, microsecond=0)

        if not date_end:
            date_end = now.replace(hour=0, minute=0, second=0, microsecond=0)

        if not isinstance(date_start, datetime.date) or not isinstance(date_end, datetime.date):
            self.logger.error("date_end and date_start must be either a string, datetime, or None.")

        if date_end.hour == 0 and date_end.minute == 0 and date_end.second == 0 and date_end.microsecond == 0:
            date_end = datetime.datetime.combine(date_end, datetime.time.max)

        return date_start, date_end

    def _get_request_func(self, request_func, next_link, http_method):
        if next_link:
            return request_func.post if http_method == "POST" else request_func.with_url(next_link).get
        else:
            return request_func.post if http_method == "POST" else request_func.get

    def _process_graph_response(self, response, f, output, output_file_path, extra_json, kwargs):
        num_events_temp = 0
        next_link = None
        odata_count = 0

        # Entity mit additional_data
        if isinstance(response, Entity) and response.additional_data.get('value', None) is not None:
            for record in response.additional_data.get('value', []):
                try:
                    num_events_temp += 1
                    if f:
                        write_json_to_file(json_input=record, file=f, extra_json=extra_json)
                    else:
                        output.append(record)
                except Exception as e:
                    self.logger.error(f"Failed to save to file {output_file_path}: {e}")

            if response.additional_data.get('@adminapi.warnings'):
                try:
                    warning_message_string = str(response.additional_data.get('@adminapi.warnings')[0])
                    self.logger.debug(warning_message_string)
                    if 'body' in kwargs:
                        kwargs['body'] = kwargs['body'].parse_pwsh_cmdlet_string(warning_message_string)
                    next_link = "NEXT LINK PROVIDED IN BODY"
                except Exception as e:
                    self.logger.error(
                        f"Failed to parse next link from @adminapi.warnings field: {response.additional_data.get('@adminapi.warnings')}. Finish export, but it might not be complete. {e}"
                    )
        else:
            if hasattr(response, "value") and response.value is not None:
                for record in response.value:
                    try:
                        num_events_temp += 1
                        if f:
                            write_json_to_file(json_input=record, file=f, extra_json=extra_json)
                        else:
                            output.append(record)
                    except Exception as e:
                        self.logger.error(f"Failed to save to file {output_file_path}: {e}")
            else:
                try:
                    num_events_temp = 1
                    if f:
                        write_json_to_file(json_input=response, file=f, extra_json=extra_json)
                    else:
                        output.append(response)
                except Exception as e:
                    self.logger.error(f"Failed to save to file {output_file_path}: {e}")

        next_link = getattr(response, "odata_next_link", next_link)
        odata_count = getattr(response, "odata_count", 0)
        return num_events_temp, next_link, odata_count

    async def __run_query(
        self,
        filter_timstamp_name: str,
        date_start: datetime.datetime,
        date_end: datetime.datetime,
        output_filename_prefix: str,
        request_func,
        delay: int,
        max_retries: int,
        encoding: str,
        custom_filter: str = None,
        filter: bool = True,
        scopes: list = DEFAULT_SCOPES,
        *args,
        **kwargs,
    ) -> None:

        date_start_identifier, date_end_identifier = date_string_in_file_identifier(date_start=date_start, date_end=date_end)
        date_identifier = "_".join(
            [date_start_identifier, date_end_identifier] if date_end_identifier else [date_start_identifier]
        )

        if filter:
            output_file_path = os.path.join(
                self.output_dir,
                f"{output_filename_prefix}_{date_identifier}{self.OUTPUT_FILE_SUFFIX}",
            )
        else:
            output_file_path = os.path.join(self.output_dir, f"{output_filename_prefix}{self.OUTPUT_FILE_SUFFIX}")

        if isinstance(request_func, str):
            request_func = [request_func]

        if not os.path.exists(output_file_path):

            for func in request_func:

                graph_client = await self._create_graph_client(self.settings.auth, scopes=scopes)
                if graph_client is None:
                    return

                # get request builder function
                try:
                    attributes = func.split(".")
                    for attr in attributes:
                        graph_client = getattr(graph_client, attr)
                except Exception:
                    self.logger.error(f"Failed to get request builder function for {func}")
                    return

                if filter or custom_filter is not None:
                    try:
                        # get request builder query function
                        query_builder_class = getattr(
                            graph_client,
                            f"{type(graph_client).__name__}GetQueryParameters",
                        )
                    except Exception:
                        self.logger.error(f"Failed to get filter query function for {type(graph_client).__name__}")
                        return

                    if custom_filter is None:
                        query_builder_class_filter = f"{filter_timstamp_name} ge {date_start.strftime('%Y-%m-%dT%H:%M:%SZ')} "
                        f"and {filter_timstamp_name} le {date_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                    else:
                        if isinstance(date_start, datetime.datetime):
                            date_start = date_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                        if isinstance(date_end, datetime.datetime):
                            date_end = date_end.strftime("%Y-%m-%dT%H:%M:%SZ")

                        query_builder_class_filter = custom_filter.format(
                            filter_timstamp_name=filter_timstamp_name,
                            date_start=date_start,
                            date_end=date_end,
                            *args,
                            **kwargs,
                        )
                        self.logger.debug(f"filter string: {query_builder_class_filter}")

                    query_params = query_builder_class(filter=query_builder_class_filter)

                    request_configuration = RequestConfiguration(
                        query_parameters=query_params,
                    )

                else:
                    request_configuration = None

                await self.make_graph_request_with_retry_and_pagination(
                    output_file_path=output_file_path,
                    request_func=graph_client,
                    delay=delay,
                    max_retries=max_retries,
                    encoding=encoding,
                    request_configuration=request_configuration,
                )

        else:
            self.logger.info(f"Output file '{output_file_path}' already exists. Skipping.")

    def create_search_identifier(self, *args):
        base_parts = []

        # Process additional parameters
        for value in args:

            value_str = str(value)

            if isinstance(value, list):
                value_str = "|".join(value)

            if value is not None and value_str:
                base_parts.append(value_str)

        # Join all parts with underscores
        return "_".join(base_parts)

    async def simple_graph_query(
        self,
        output_filename_prefix: str,
        request_func,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
        split_days: bool = False,
        delay: int = 60,
        max_retries: int = 10,
        encoding: str = "utf-8",
        filter: bool = False,
        filter_timstamp_name: str = "",
        scopes: list = ["https://graph.microsoft.com/.default"],
        custom_filter: str = None,
        number_interval_days: int = 7,
        *args,
        **kwargs,
    ) -> None:
        if not date_start and not date_end:
            date_start, date_end = self._read_date_fields()

        if split_days:
            filter = True

        tasks = []
        if split_days:
            semaphore = asyncio.Semaphore(5)
            for tmp_date_start, tmp_date_end in daterange(date_start, date_end, number_interval_days):
                tasks.append(
                    semaphore_wrapper(
                        function=self.__run_query,
                        filter_timstamp_name=filter_timstamp_name,
                        date_start=tmp_date_start,
                        date_end=tmp_date_end,
                        output_filename_prefix=output_filename_prefix,
                        request_func=request_func,
                        delay=delay,
                        max_retries=max_retries,
                        encoding=encoding,
                        filter=filter,
                        semaphore=semaphore,
                        scopes=scopes,
                        custom_filter=custom_filter,
                        *args,
                        **kwargs,
                    )
                )
        else:
            tasks.append(
                semaphore_wrapper(
                    function=self.__run_query,
                    filter_timstamp_name=filter_timstamp_name,
                    date_start=date_start,
                    date_end=date_end,
                    output_filename_prefix=output_filename_prefix,
                    request_func=request_func,
                    delay=delay,
                    max_retries=max_retries,
                    filter=filter,
                    encoding=encoding,
                    scopes=scopes,
                    custom_filter=custom_filter,
                    *args,
                    **kwargs,
                )
            )

        await asyncio.gather(*tasks)

    async def make_graph_request_for_child_items(
        self,
        parent,
        child,
        identifier_function,
        identifier="id",
        parent_fields_to_child=[],
        output_filename=None,
    ):
        if output_filename is None:
            output_filename = f"{parent}_{child}"

        # Construct output file path
        output_file_path = os.path.join(self.output_dir, f"{output_filename}.json")

        if not os.path.exists(output_file_path):

            await self.ensure_graph_client(scopes=self.DEFAULT_SCOPES)
            if self.graph_client is None:
                return

            graph_client = await self._create_graph_client(self.settings.auth)
            if graph_client is None:
                return

            # Traverse parent attributes to get the request builder function
            try:
                parent_request_builder = graph_client
                for attr in parent.split("."):
                    parent_request_builder = getattr(parent_request_builder, attr)
            except AttributeError as e:
                self.logger.error(f"Failed to get request builder function for {parent}: {e}")
                return

            # Fetch parent objects
            parent_response_values = await self.make_graph_request_with_retry_and_pagination(
                output_file_path=None, request_func=parent_request_builder
            )

            # Process each parent object
            for parent_response_value in parent_response_values:
                try:
                    identifier_value = getattr(parent_response_value, identifier)
                    identifier_function_method = getattr(parent_request_builder, identifier_function)
                    child_graph_client = identifier_function_method(identifier_value)
                except AttributeError as e:
                    self.logger.error(f"Failed to get identifier function {identifier_function} for {parent}: {e}")
                    continue
                except Exception as e:
                    self.logger.error(f"Unexpected error when accessing identifier function {identifier_function}: {e}")
                    continue

                # Traverse child attributes to get the function
                try:
                    child_request_builder = child_graph_client
                    for attr in child.split("."):
                        # Handle dynamic method calls
                        if "(" in attr and ")" in attr:
                            method_name, method_args = attr.split("(")
                            method_args = method_args.strip(")").strip("'\"")
                            child_request_builder = getattr(child_request_builder, method_name)(method_args)
                        else:
                            child_request_builder = getattr(child_request_builder, attr)
                except AttributeError as e:
                    self.logger.error(f"Failed to get function for {child}: {e}")
                    continue

                # Fetch child objects
                try:
                    extra_json = {}
                    for key, value in (
                        parent_fields_to_child.items()
                        if not isinstance(parent_fields_to_child, list)
                        else parent_fields_to_child
                    ):
                        try:
                            extra_json[key] = getattr(parent_response_value, value)
                        except Exception:
                            self.logger.debug(
                                f"Could not pass field {value} from parent "
                                f"{parent}.{identifier_function}('{identifier_value}') to child"
                            )

                    await self.make_graph_request_with_retry_and_pagination(
                        output_file_path=output_file_path,
                        request_func=child_request_builder,
                        fail=True,
                        extra_json=extra_json,
                    )
                except ODataError as e1:
                    self.logger.warning(
                        f"Failed to download data {parent}.{identifier_function}('{identifier_value}').{child}: "
                        f"{e1.error.message}"
                    )
                except Exception as e2:
                    self.logger.error(
                        f"Failed to download data {parent}.{identifier_function}('{identifier_value}').{child}: " f"{e2}"
                    )
        else:
            self.logger.info(f"Output file '{output_file_path}' already exists. Skipping.")

    async def make_graph_request_with_retry(
        self,
        request_func,
        delay: int = 60,
        max_retries: int = 10,
        *args,
        **kwargs,
    ) -> object:
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = await request_func(*args, **kwargs)

                return response
            except ODataError as e:
                retry_count += 1
                retry_after = int(e.response_headers.get("Retry-After", delay))

                if e.response_status_code == 429:
                    self.logger.warning(f"Received 429 Too Many Requests. Retrying in {retry_after} seconds...")
                elif e.response_status_code == 500:
                    if hasattr(e, "message"):
                        if hasattr(e, "additional_data"):
                            error_message = (
                                e.error.additional_data.get('innererror', {"internalexception": ""})
                                .get('internalexception', {"message": ""})
                                .get('message', "-")
                            )
                            if (
                                error_message
                                == "Invalid StartDate value. The StartDate can't be older than 90 days from today."
                            ):
                                raise Exception(error_message + " Try to download other date frames.")
                            elif error_message != "-":
                                self.logger.warning(
                                    f"Received 500 Server error: {error_message}. Retrying in {retry_after} seconds..."
                                )
                            else:
                                self.logger.warning(
                                    f"Received 500 Server error: {e.error.additional_data}. Retrying in {retry_after} seconds..."
                                )
                        else:
                            self.logger.warning(
                                f"Received 500 Server error: {e.error.message}. Retrying in {retry_after} seconds..."
                            )
                    else:
                        self.logger.warning(f"Received 500 Server error. Retrying in {retry_after} seconds...")
                elif e.response_status_code == 503:
                    if hasattr(e, "message"):
                        self.logger.warning(
                            f"Received 503 The service is unavailable: {e.error.message}. Retrying in {retry_after} seconds..."
                        )
                    else:
                        self.logger.warning(f"Received 503 The service is unavailable. Retrying in {retry_after} seconds...")
                else:
                    raise

                await asyncio.sleep(retry_after)
            except JSONDecodeError:
                retry_count += 1
                self.logger.error(
                    "Received JSON decode error. Seems like something is wrong with the GraphAPI. "
                    "Delete the request ID file and start magic again."
                )
                raise
            except Exception:
                raise

        raise Exception("Max retries exceeded for request")

    async def make_graph_request_with_retry_and_pagination(
        self,
        output_file_path: str,
        request_func,
        delay: int = 60,
        max_retries: int = 10,
        encoding: str = "utf-8",
        next_link: str = None,
        fail: bool = False,
        extra_json: dict = None,
        http_method: str = "GET",
        *args,
        **kwargs,
    ):
        num_events = 0
        output = []
        f = None
        if output_file_path is not None:
            f = open(output_file_path, "a", encoding=encoding)

        while True:
            response = None
            try:
                req_func = self._get_request_func(request_func, next_link, http_method)
                try:
                    response = await self.make_graph_request_with_retry(
                        request_func=req_func,
                        delay=delay,
                        max_retries=max_retries,
                        *args,
                        **kwargs,
                    )
                except Exception as e:
                    if fail:
                        raise
                    else:
                        self.logger.error(
                            f"{e} - Failed to download data in function {request_func} {'from next_link' if next_link else ''}"
                        )
                        break

                if response is not None:
                    num_events_temp, next_link, odata_count = self._process_graph_response(
                        response, f, output, output_file_path, extra_json, kwargs
                    )
                    num_events += num_events_temp

                    if not next_link:
                        self.logger.debug(
                            f"{num_events_temp}/{odata_count} audit log records have been saved to {output_file_path}. "
                            "Finished downloading, because no next link was provided"
                        )
                        break
                    else:
                        self.logger.debug(
                            f"{num_events_temp}/{odata_count} audit log records have been saved to {output_file_path}. "
                            "Download more results, since a next link was provided"
                        )
                else:
                    self.logger.error("Failed to further process, because response had an error")
                    break
            except Exception as e:
                if fail:
                    raise
                else:
                    self.logger.error(f"Unexpected error occurred: {e}")

        if output_file_path is None:
            return output
        else:
            f.close()
            self.logger.debug(
                f"{num_events} records have been saved to {output_file_path} from request function {request_func.__module__}"
            )
