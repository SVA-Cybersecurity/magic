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
from ..interfaces.crawler import BaseCrawler
from ..helpers.utils import TaskWrapper
from ..helpers.registry import register_crawler
from ..helpers.permissions import require_permissions, ServicePrincipalType
from ..helpers.config import RETENTION_MESSAGE_TRACES


@register_crawler(name="m365_message_traces")
class MessageTracesCrawler(BaseCrawler):

    def __init__(self, **kwargs):
        self.RETENTION = RETENTION_MESSAGE_TRACES
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> list[TaskWrapper]:
        self.logger.debug(f"Get message trace logs task with params: {self.config.model_dump()}")

        tasks = []
        self.logger.debug("Create message trace logs task")
        tasks.append(TaskWrapper(name="crawl_message_traces", coroutine=self.crawl_message_traces()))

        return tasks

    @require_permissions([(ServicePrincipalType.GRAPH_API, "AuditLog.Read.All")])  # TODO check permission
    async def crawl_message_traces(self, split_days: bool = True) -> None:

        date_start, date_end = self._read_date_fields()

        self.logger.info(
            f"Get message trace logs from {date_start} to {date_end}"
            + (f" for sender {self.config.sender_address}" if self.config.sender_address else "")
            + (f" for recipient {self.config.recipient_address}" if self.config.recipient_address else "")
        )

        custom_filter = self.build_odata_filter(
            senderAddress=self.config.sender_address,
            recipientAddress=self.config.recipient_address,
            fromIP=self.config.from_ip,
            toIP=self.config.to_ip,
        )

        if self.config.subject and self.config.subject_filter_type:
            custom_filter += f" and subject {self.config.subject_filter_type} '{self.config.subject}'"

        self.logger.debug(f"crawl_message_traces with filter {custom_filter}")

        output_filename_prefix = self.create_search_identifier(
            "message_traces",
            "from-" + self.config.sender_address if self.config.sender_address else "",
            "to-" + self.config.recipient_address if self.config.recipient_address else "",
            "from-" + self.config.from_ip if self.config.from_ip else "",
            "to-" + self.config.to_ip if self.config.to_ip else "",
            (
                "subject-" + self.config.subject
                if self.config.subject
                else "" "subject_filter_type-" + self.config.subject_filter_type if self.config.subject_filter_type else ""
            ),
        )

        await self.simple_graph_query(
            filter_timstamp_name="receivedDateTime",
            output_filename_prefix=output_filename_prefix,
            request_func="admin.exchange.message_traces",
            date_start=date_start,
            date_end=date_end,
            split_days=split_days,
            custom_filter=custom_filter,
            number_interval_days=self.config.number_interval_days,
        )
