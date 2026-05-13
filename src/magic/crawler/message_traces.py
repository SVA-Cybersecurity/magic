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
from typing import List
from ..interfaces.crawler import BaseCrawler
from ..helpers.utils import TaskWrapper, daterange
from ..helpers.registry import register_crawler
from ..helpers.permissions import require_permissions, ServicePrincipalType
from ..helpers.config import RETENTION_MESSAGE_TRACES
from datetime import datetime, timedelta


@register_crawler(name="m365_message_traces")
class MessageTracesCrawler(BaseCrawler):

    def __init__(self, **kwargs):
        self.RETENTION = RETENTION_MESSAGE_TRACES
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> List[TaskWrapper]:
        self.logger.debug(f"Create crawl_message_traces task with params: {self.config.model_dump()}")

        date_start, date_end = self._read_date_fields()

        time_between_now_and_retention = datetime.now() - timedelta(days=self.RETENTION)

        tasks = []
        if time_between_now_and_retention > date_start:
            if time_between_now_and_retention > date_end:
                self.logger.error(
                    f"Start date and end date is older than {self.RETENTION} days. Skipping message trace crawling."
                )
                return
            else:
                date_start = (datetime.now() - timedelta(days=self.RETENTION)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                self.logger.warning(f"Start date is older than {self.RETENTION} days. Set new start date to {date_start}")

        senders = self.config.sender_addresses
        recipients = self.config.recipient_addresses

        for tmp_date_start, tmp_date_end in daterange(date_start, date_end, self.config.number_interval_days):

            if not senders and not recipients:
                continue

            if not senders and recipients:
                for recipient in recipients:
                    tasks.append(
                        TaskWrapper(
                            name=f"crawl_message_traces_to_{recipient}",
                            coroutine=self.crawl_message_traces(tmp_date_start, tmp_date_end, None, recipient),
                        )
                    )

            if not recipients and senders:
                for sender in senders:
                    tasks.append(
                        TaskWrapper(
                            name=f"crawl_message_traces_from_{sender}",
                            coroutine=self.crawl_message_traces(tmp_date_start, tmp_date_end, sender, None),
                        )
                    )

            if senders and recipients:
                for sender in senders:
                    for recipient in recipients:
                        tasks.append(
                            TaskWrapper(
                                name=f"crawl_message_traces_from_{sender}_to_{recipient}",
                                coroutine=self.crawl_message_traces(tmp_date_start, tmp_date_end, sender, recipient),
                            )
                        )

        return tasks

    @require_permissions([(ServicePrincipalType.GRAPH_API, "ExchangeMessageTrace.Read.All")])
    async def crawl_message_traces(
        self, date_start, date_end, sender_address, recipient_address, split_days: bool = True
    ) -> None:
        self.logger.info(
            f"Get message trace logs from {date_start} to {date_end}"
            + (f" for sender {sender_address}" if sender_address else "")
            + (f" for recipient {recipient_address}" if recipient_address else "")
        )

        custom_filter = self.build_odata_filter(
            senderAddress=sender_address,
            recipientAddress=recipient_address,
            fromIP=self.config.from_ip,
            toIP=self.config.to_ip,
        )

        if self.config.subject and self.config.subject_filter_type:
            custom_filter += f" and {self.config.subject_filter_type}(subject, '{self.config.subject}')"

        self.logger.debug(f"crawl_message_traces with filter {custom_filter}")

        output_filename_prefix = self.create_search_identifier(
            "message_traces",
            "from-" + sender_address if sender_address else "",
            "to-" + recipient_address if recipient_address else "",
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
            request_func="admin.exchange.tracing.message_traces",
            date_start=date_start,
            date_end=date_end,
            split_days=split_days,
            custom_filter=custom_filter,
            number_interval_days=self.config.number_interval_days,
        )
