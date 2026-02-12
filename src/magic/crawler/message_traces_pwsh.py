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
from typing import List
from ..interfaces.crawler import BaseCrawler
from ..helpers.utils import TaskWrapper, date_string_in_file_identifier, daterange
from ..helpers.registry import register_crawler
from ..helpers.permissions import require_permissions, ServicePrincipalType
from ..helpers.config import RETENTION_MESSAGE_TRACES
from ..helpers.pwsh import PowerShellModuleRequestBuilder, CmdletRootModel, CmdletParameters, CmdletInput
from datetime import datetime, timedelta


@register_crawler(name="m365_message_traces_pwsh")
class MessageTracesPWSHCrawler(BaseCrawler):

    OUTPUT_FILE_SUFFIX = ".json"

    def __init__(self, **kwargs):
        self.RETENTION = RETENTION_MESSAGE_TRACES
        self.DEFAULT_SCOPES = ["https://outlook.office365.com/.default"]  # override default scope

        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> List[TaskWrapper]:
        self.logger.debug(f"Create crawl_message_traces pwsh task with params: {self.config.model_dump()}")

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
                            name=f"crawl_message_traces_pwsh_to_{recipient}",
                            coroutine=self.crawl_message_traces_pwsh(tmp_date_start, tmp_date_end, None, recipient),
                        )
                    )

            if not recipients and senders:
                for sender in senders:
                    tasks.append(
                        TaskWrapper(
                            name=f"crawl_message_traces_pwsh_from_{sender}",
                            coroutine=self.crawl_message_traces_pwsh(tmp_date_start, tmp_date_end, sender, None),
                        )
                    )

            if senders and recipients:
                for sender in senders:
                    for recipient in recipients:
                        tasks.append(
                            TaskWrapper(
                                name=f"crawl_message_traces_pwsh_from_{sender}_to_{recipient}",
                                coroutine=self.crawl_message_traces_pwsh(tmp_date_start, tmp_date_end, sender, recipient),
                            )
                        )

        return tasks

    @require_permissions([(ServicePrincipalType.O365_EXCHANGE, "Exchange.ManageAsApp")])
    async def crawl_message_traces_pwsh(self, date_start, date_end, sender_address, recipient_address):
        date_start, date_end = self._read_date_fields()

        self.logger.info(
            f"Get message trace logs via powershell from {date_start} to {date_end}"
            + (f" for sender {sender_address}" if sender_address else "")
            + (f" for recipient {recipient_address}" if recipient_address else "")
        )

        date_start_identifier, date_end_identifier = date_string_in_file_identifier(date_start, date_end)

        search_identifier = self.create_search_identifier(
            "message_traces_pwsh",
            "from-" + sender_address if sender_address else "",
            "to-" + recipient_address if recipient_address else "",
            "from-" + self.config.from_ip if self.config.from_ip else "",
            "to-" + self.config.to_ip if self.config.to_ip else "",
            "subject-" + self.config.subject if self.config.subject else "",
            date_start_identifier,
            date_end_identifier,
        )

        output_file_path = os.path.join(self.output_dir, f"{search_identifier}{self.OUTPUT_FILE_SUFFIX}")

        if not os.path.exists(output_file_path):

            self.graph_client = await self._create_graph_client(self.settings.auth, self.DEFAULT_SCOPES)
            if self.graph_client is None:
                return

            await self.make_graph_request_with_retry_and_pagination(
                http_method="POST",
                output_file_path=output_file_path,
                request_func=PowerShellModuleRequestBuilder(
                    self.graph_client.request_adapter, path_parameters={"tenant_id": self.settings.auth.tenant_id}
                ),
                body=CmdletRootModel(
                    CmdletInput(
                        cmdlet_name="Get-MessageTraceV2",
                        parameters=CmdletParameters(
                            allowed_fields=[
                                "StartDate",
                                "EndDate",
                                "ResultSize",
                                "RecipientAddress",
                                "SenderAddress",
                                "FromIP",
                                "Subject",
                                "ToIP",
                                "SubjectFilterType",
                                "StartingRecipientAddress",
                            ],
                            EndDate=date_end.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                            StartDate=date_start.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                            ResultSize=self.config.result_size,
                            SenderAddress=sender_address,
                            RecipientAddress=recipient_address,
                            FromIP=self.config.from_ip,
                            ToIP=self.config.to_ip,
                            Subject=self.config.subject,
                            SubjectFilterType=self.config.subject_filter_type,
                        ),
                    )
                ),
            )

        else:
            self.logger.info(f"Output file '{output_file_path}' already exists. Skipping.")
