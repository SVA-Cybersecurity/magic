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
from kiota_abstractions.native_response_handler import NativeResponseHandler
from kiota_http.middleware.options import ResponseHandlerOption
from msgraph_beta.generated.users.item.messages.item.message_item_request_builder import MessageItemRequestBuilder
from msgraph_beta.generated.users.item.messages.messages_request_builder import MessagesRequestBuilder
from ..interfaces.crawler import BaseCrawler
from ..helpers.utils import TaskWrapper
from ..helpers.registry import register_crawler
from ..helpers.permissions import require_permissions, ServicePrincipalType


@register_crawler(name="m365_message")
class MessageCrawler(BaseCrawler):

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    async def _get_message_id(self) -> str:
        request_configuration = MessagesRequestBuilder.MessagesRequestBuilderGetRequestConfiguration(
            query_parameters=MessagesRequestBuilder.MessagesRequestBuilderGetQueryParameters(
                select=['id'], filter=f"internetMessageId eq \'{self.config.internet_message_id}\'"
            )
        )

        message_temp = await self.graph_client.users.by_user_id(self.config.user_principal_name).messages.get(
            request_configuration=request_configuration
        )

        # check RecoverableItemsDeletions, RecoverableItemsPurges, RecoverableItemsDiscoveryHolds folder
        # https://learn.microsoft.com/en-us/exchange/security-and-compliance/recoverable-items-folder/recoverable-items-folder
        folders = [
            'RecoverableItemsDeletions',
            'RecoverableItemsPurges',
            'RecoverableItemsDiscoveryHolds',
            'RecoverableItemsVersions',
        ]
        for folder in folders:
            if not message_temp.value:

                message_temp = (
                    await self.graph_client.users.by_user_id(self.config.user_principal_name)
                    .mail_folders.by_mail_folder_id(folder)
                    .messages.get(request_configuration=request_configuration)
                )

        return message_temp.value[0].id

    def get_tasks(self) -> List[TaskWrapper]:
        self.logger.debug(f"Create crawl_message task with params: {self.config.model_dump()}")

        return [TaskWrapper(name="crawl_message", coroutine=self.crawl_message())]

    @require_permissions([(ServicePrincipalType.GRAPH_API, "Mail.Read")])
    async def crawl_message(self) -> None:
        await self.ensure_graph_client()
        if self.graph_client is None:
            return

        request_config = MessageItemRequestBuilder.MessageItemRequestBuilderGetRequestConfiguration(
            options=[ResponseHandlerOption(NativeResponseHandler())],
        )

        if not self.config.message_id:
            try:
                self.config.message_id = await self._get_message_id()
            except Exception as e:
                self.logger.error(
                    f"Failed to get message_id from internet_message_id '{self.config.internet_message_id}'. {e}"
                )
                return

        if self.config.message_id:
            output_path = '/'.join([self.output_dir, self.config.message_id])

            if not self._check_output_file_exists(output_path):
                try:
                    message = await (
                        self.graph_client.users.by_user_id(self.config.user_principal_name)
                        .messages.by_message_id(self.config.message_id)
                        .with_url(
                            f'https://graph.microsoft.com/beta/users/{self.config.user_principal_name}/messages/{self.config.message_id}/$value'
                        )
                        .get(request_configuration=request_config)
                    )
                    message.raise_for_status()

                    with open(output_path, 'wb') as file:
                        file.write(message.content)

                except Exception as e:
                    self.logger.error(
                        f"Failed to download message with message_id '{self.config.message_id}'."
                        + (
                            f"internet_message_id was '{self.config.internet_message_id}'."
                            if self.config.internet_message_id
                            else ""
                        )
                        + f"{e}"
                    )
