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
from msgraph_beta.generated.users.item.messages.messages_request_builder import MessagesRequestBuilder
from ..interfaces.crawler import BaseCrawler
from ..helpers.config import RETENTION_MESSAGES
from ..helpers.registry import register_crawler
from ..helpers.permissions import require_permissions, ServicePrincipalType
from ..helpers.utils import TaskWrapper, date_string_in_file_identifier


@register_crawler(name="m365_messages")
class MessagesCrawler(BaseCrawler):

    OUTPUT_FILE_SUFFIX = ".json"

    def __init__(self, **kwargs):
        self.RETENTION = RETENTION_MESSAGES
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> List[TaskWrapper]:
        self.logger.debug(f"Get crawl_messages tasks with configured crawl params: {self.config.model_dump()}")

        tasks = []
        for user_principal_name in self.config.user_principal_names:
            self.logger.debug(f"Create crawl_messages task for user: {user_principal_name}")
            tasks.append(TaskWrapper(name="crawl_messages", coroutine=self.crawl_messages(user_principal_name)))

        return tasks

    @require_permissions([(ServicePrincipalType.GRAPH_API, "Mail.Read")])
    async def crawl_messages(self, user_principal_name: str) -> None:
        fields_to_select = [
            'internetMessageId',
            'createdDateTime',
            'receivedDateTime',
            'sentDateTime',
            'subject',
            'sender',
            'from',
            'toRecipients',
            'ccRecipients',
            'bccRecipients',
            'replyTo',
            'parentFolderId',
        ]

        filter = None
        date_start, date_end = self._read_date_fields()

        if date_start is not None or date_end is not None:
            self.logger.info(f"Get messages from {date_start} to {date_end} for user {user_principal_name}")
            filter = f"createdDateTime ge {date_start.strftime('%Y-%m-%dT%H:%M:%SZ')} and createdDateTime le {date_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        else:
            self.logger.info(f"Get messages for user {user_principal_name}")

        date_start_identifier, date_end_identifier = date_string_in_file_identifier(date_start, date_end)

        search_identifier = self.create_search_identifier(
            "messages", user_principal_name, date_start_identifier, date_end_identifier
        )

        output_file_path = os.path.join(self.output_dir, f"{search_identifier}{self.OUTPUT_FILE_SUFFIX}")

        request_configuration = MessagesRequestBuilder.MessagesRequestBuilderGetRequestConfiguration(
            query_parameters=MessagesRequestBuilder.MessagesRequestBuilderGetQueryParameters(
                filter=filter, select=fields_to_select
            )
        )

        if not os.path.exists(output_file_path):
            self.graph_client = await self._create_graph_client(self.settings.auth, self.DEFAULT_SCOPES)
            if self.graph_client is None:
                return

            await self.make_graph_request_with_retry_and_pagination(
                output_file_path=output_file_path,
                request_func=self.graph_client.users.by_user_id(user_principal_name).messages,
                request_configuration=request_configuration,
            )

        else:
            self.logger.info(f"Output file '{output_file_path}' already exists. Skipping.")
