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
import json
import os
import datetime
from msgraph_beta.generated.models.security.audit_log_query import AuditLogQuery
from msgraph_beta.generated.models.security.audit_log_query_status import AuditLogQueryStatus
from msgraph_beta.generated.models.security.audit_log_record import AuditLogRecord
from typing import List
from ..interfaces.crawler import BaseCrawler
from ..helpers.config import RETENTION_UAL
from ..helpers.registry import register_crawler
from ..helpers.permissions import require_permissions, ServicePrincipalType
from ..helpers.utils import TaskWrapper, daterange, date_string_in_file_identifier, custom_serializer


@register_crawler(name="m365_ual")
class UalCrawler(BaseCrawler):

    OUTPUT_FILE_SUFFIX = ".json"

    def __init__(self, **kwargs):
        self.RETENTION = RETENTION_UAL
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> List[TaskWrapper]:
        self.logger.debug(f"Create crawl_ual task with params: {self.config.model_dump()}")

        return [TaskWrapper(name="crawl_ual", coroutine=self.crawl_ual())]

    @require_permissions(
        [(ServicePrincipalType.GRAPH_API, "AuditLogsQuery.Read.All"), (ServicePrincipalType.GRAPH_API, "AuditLog.Read.All")]
    )
    async def crawl_ual(self, encoding: str = "utf-8") -> None:

        await self.ensure_graph_client()
        if self.graph_client is None:
            return

        date_start, date_end = self._read_date_fields()
        temp_user_list = ", ".join((self.config.user_principal_names if self.config.user_principal_names else []))
        self.logger.info(
            f"Get unified audit logs from {date_start} to {date_end}"
            + (f" for users {temp_user_list}" if self.config.user_principal_names else "")
        )

        searches = await self._create_searches(date_start, date_end)

        retry_after = 120
        searches_without_none = [x for x in searches if x is not None]
        while len(searches_without_none) > 0:

            for id, name, identifier in searches_without_none:

                output_filename = "ual" + self.OUTPUT_FILE_SUFFIX

                if identifier != "":
                    output_filename = f"ual_{identifier}" + self.OUTPUT_FILE_SUFFIX

                output_file_path = os.path.join(self.output_dir, output_filename)

                try:
                    audit_log_query = await self.make_graph_request_with_retry(
                        self.graph_client.security.audit_log.queries.by_audit_log_query_id(id).get
                    )
                except Exception:
                    self.logger.error(
                        f"Could not download results for Unified Audit Log Search {name} "
                        f"with id {id} because job did not finish"
                    )
                    searches_without_none.remove((id, name, identifier))
                    continue

                match audit_log_query.status:
                    case AuditLogQueryStatus.Running:
                        self.logger.info(
                            f"Unified Audit Log Search {name} is still running. Retrying in {retry_after} seconds..."
                        )
                        continue

                    case AuditLogQueryStatus.NotStarted:
                        self.logger.info(
                            f"Unified Audit Log Search {name} did not start yet. Retrying in {retry_after} seconds..."
                        )
                        continue

                    case AuditLogQueryStatus.Failed:
                        self.logger.error(f"Unified Audit Log Search {name} failed")
                        searches_without_none.remove((id, name, identifier))
                        continue

                    case AuditLogQueryStatus.Succeeded:
                        self.logger.info(f"Unified Audit Log Search {name} succeeded. Start to download results")
                        await self.download_ual(
                            search_name=name, query_id=id, encoding=encoding, output_file_path=output_file_path
                        )
                        searches_without_none.remove((id, name, identifier))
                        continue

                    case _:
                        self.logger.error(
                            f"Unified Audit Log Search {name} is in an undefined status and can not be further processed."
                        )
                        searches_without_none.remove((id, name, identifier))
                        continue

            if len(searches_without_none) > 0:
                self.logger.info(
                    f"No finished searches available for download, but still {len(searches_without_none)} running searches"
                    f" in the pipeline. Retrying in {retry_after} seconds..."
                )
                await asyncio.sleep(retry_after)

    async def _create_searches(
        self, date_start: datetime.datetime, date_end: datetime.datetime, split_days: bool = True
    ) -> List[tuple]:

        searches = []
        number_interval_days = 0

        if split_days:
            number_interval_days = self.config.number_interval_days

        for tmp_date_start, tmp_date_end in daterange(date_start, date_end, number_interval_days):

            date_start_identifier, date_end_identifier = date_string_in_file_identifier(tmp_date_start, tmp_date_end)

            search_identifier = self.create_search_identifier(
                "ual",
                (
                    [user.split("@")[0] for user in self.config.user_principal_names]
                    if self.config.user_principal_names is not None
                    else None
                ),
                date_start_identifier,
                date_end_identifier,
            )

            try:
                searches.append(await self._create_search(search_identifier, tmp_date_start, tmp_date_end))
            except Exception as e:
                self.logger.error(f"Failed to create search with identifier {search_identifier}: {e}")

        return searches

    async def _create_search(
        self, search_identifier: str, date_start: datetime.datetime, date_end: datetime.datetime
    ) -> tuple:

        output_file_path = os.path.join(self.output_dir, f"{search_identifier}{self.OUTPUT_FILE_SUFFIX}")

        query_id_file_path = os.path.join(self.output_dir, f"{search_identifier}_query_id.txt")

        if os.path.exists(output_file_path):
            self.logger.info(
                f"Output file {output_file_path} already exists. Skipping Unified Audit Log Search {search_identifier}"
            )
            return

        if os.path.exists(query_id_file_path):
            self.logger.info(
                f"Request Id file {query_id_file_path} already exists. Skipping Unified Audit Log Search {search_identifier}"
            )
            with open(query_id_file_path, "r") as qid_file:
                query_id = qid_file.read()

            await self.ensure_graph_client()
            if self.graph_client is None:
                return
        else:
            params = AuditLogQuery(
                odata_type="#microsoft.graph.security.auditLogQuery",
                display_name=f"{self.config.search_name_prefix}{search_identifier}",
                filter_start_date_time=date_start.isoformat(),
                filter_end_date_time=date_end.isoformat(),
                record_type_filters=self.config.record_types,
                keyword_filter=self.config.keyword,
                operation_filters=self.config.operations,
                user_principal_name_filters=self.config.user_principal_names,
                ip_address_filters=self.config.ip_addresses,
                object_id_filters=self.config.object_id_filters,
                administrative_unit_id_filters=self.config.administrative_unit_id_filters,
                status=AuditLogQueryStatus.NotStarted,
                additional_data={"service_filter": self.config.service},
            )

            try:
                await self.ensure_graph_client()
                if self.graph_client is None:
                    return

                start_scan = await self.make_graph_request_with_retry(
                    request_func=self.graph_client.security.audit_log.queries.post,
                    body=params,
                )
                query_id = start_scan.id
                self.logger.info(f"New Unified Audit Log Search started with name {search_identifier} and id {query_id}")
            except Exception as e:
                self.logger.error(f"Could not create new Unified Audit Log Search with name {search_identifier}: {e}")
                return

            # Save request Id to a file
            with open(query_id_file_path, "w") as qid_file:
                qid_file.write(query_id)

        return (query_id, search_identifier, search_identifier)

    async def download_ual(self, search_name, query_id, encoding, output_file_path):

        await self.ensure_graph_client()
        if self.graph_client is None:
            return

        total_events = 0

        with open(output_file_path, "w", encoding=encoding) as f:
            next = None

            while True:
                try:
                    request_func = self.graph_client.security.audit_log.queries.by_audit_log_query_id(query_id).records.get

                    if next:
                        request_func = self.graph_client.security.audit_log.queries.with_url(next).get

                    try:
                        response = await self.make_graph_request_with_retry(request_func)
                    except Exception as e:
                        self.logger.error(f"Failed to download data to output file {output_file_path}: {e}")

                    if response:
                        batch_events = 0

                        for record in response.value:
                            try:
                                data = None
                                match record:
                                    case AuditLogRecord():
                                        data = record.audit_data.additional_data
                                    case AuditLogQuery():
                                        data = record.additional_data.get("auditData")
                                if data:
                                    try:
                                        json.dump(
                                            data,
                                            f,
                                            ensure_ascii=False,
                                            default=custom_serializer,
                                        )
                                        f.write("\n")
                                    except Exception as e:
                                        self.logger.error(
                                            f"Failed to save {record.__class__.__name__} to file {output_file_path}: {e}"
                                        )
                                else:
                                    self.logger.error(record)
                            except Exception as e:
                                self.logger.error(f"Failed to save to file {output_file_path}: {e}")

                            batch_events += 1

                        total_events += batch_events
                        next = response.odata_next_link
                        odata_count = response.odata_count

                        if not next:
                            self.logger.debug(
                                f"{batch_events}/{odata_count} audit log records have been saved to {output_file_path}. "
                                "Finished downloading, because no next link was provided"
                            )
                            break
                        else:
                            self.logger.debug(
                                f"{batch_events}/{odata_count} audit log records have been saved to {output_file_path}. "
                                "Download more results, since a next link was provided"
                            )

                    else:
                        self.logger.error("Failed to further process, because response had an error")
                        break

                except Exception as e:
                    self.logger.error(f"Unexpected error occurred: {e}")

        self.logger.info(
            f"Downloading Unified Audit Log Search {search_name} finished with downloading {total_events} "
            f"audit log records to {output_file_path}."
        )
