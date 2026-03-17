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
from ..helpers.config import RETENTION_SIGN_IN
from ..helpers.registry import register_crawler
from ..helpers.permissions import require_permissions, ServicePrincipalType


@register_crawler(name="m365_signin")
class SignInCrawler(BaseCrawler):

    def __init__(self, **kwargs):
        self.RETENTION = RETENTION_SIGN_IN
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> list[TaskWrapper]:
        self.logger.debug(f"Get crawl_signin task with params: {self.config.model_dump()}")

        tasks = []
        if self.config.user_principal_names:
            tasks = []
            for user_principal_name in self.config.user_principal_names:
                self.logger.debug(f"Create crawl_signin tasks for user: {user_principal_name}")
                tasks.append(
                    TaskWrapper(name=f"crawl_signin_{user_principal_name}", coroutine=self.crawl_signin(user_principal_name))
                )
        else:
            self.logger.debug("Create crawl_signin task")
            tasks.append(TaskWrapper(name="crawl_signin", coroutine=self.crawl_signin()))

        return tasks

    @require_permissions([(ServicePrincipalType.GRAPH_API, "AuditLog.Read.All")])
    async def crawl_signin(self, user_principal_name: str | None = None, split_days: bool = True) -> None:

        date_start, date_end = self._read_date_fields()

        self.logger.info(
            f"Get {self.config.sign_in_type.value} sign in logs from {date_start} to {date_end}"
            + (f" for user {user_principal_name}" if user_principal_name else "")
        )

        custom_filter = self.build_odata_filter(
            userPrincipalName=user_principal_name.lower(),
        )

        if custom_filter:
            custom_filter = self.config.sign_in_type.odata_filter + " and " + custom_filter
        else:
            custom_filter = self.config.sign_in_type.odata_filter

        self.logger.debug(f"crawl_signin with filter {custom_filter}")

        output_filename_prefix = self.create_search_identifier("sign_ins", self.config.sign_in_type.value, user_principal_name)

        await self.simple_graph_query(
            filter_timstamp_name="createdDateTime",
            output_filename_prefix=output_filename_prefix,
            request_func="audit_logs.sign_ins",
            date_start=date_start,
            date_end=date_end,
            split_days=split_days,
            custom_filter=custom_filter,
            number_interval_days=self.config.number_interval_days,
        )
