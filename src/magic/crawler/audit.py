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
from ..helpers.utils import TaskWrapper
from ..helpers.registry import register_crawler
from ..helpers.config import RETENTION_AUDIT
from ..helpers.permissions import require_permissions, ServicePrincipalType


@register_crawler(name="m365_audit")
class AuditCrawler(BaseCrawler):

    def __init__(self, **kwargs):
        self.RETENTION = RETENTION_AUDIT
        super().__init__(**kwargs, logger=__name__)

    async def _build_custom_filter(self, user_principal_name: str) -> str:
        base_filter = "({filter_timstamp_name} ge {date_start} and {filter_timstamp_name} le {date_end})"
        if user_principal_name:
            user_id = await self._get_user_id(user_principal_name)
            if user_id:
                self.logger.debug(f"User '{user_principal_name}' has ID '{user_id}'")
                return f"{base_filter} and (initiatedBy/user/id eq '{user_id}' or targetResources/any(c:c/id eq '{user_id}') or initiatedBy/user/userPrincipalName eq '{user_principal_name}' or targetResources/any(c:c/displayName eq '{user_principal_name}')"
            else:
                self.logger.debug(f"Did not find ID for user with name '{user_principal_name}'")
                return f"{base_filter} and (initiatedBy/user/userPrincipalName eq '{user_principal_name}') or targetResources/any(c:c/displayName eq '{user_principal_name}')"
        else:
            return base_filter

    def get_tasks(self) -> List[TaskWrapper]:
        self.logger.debug(f"Get crawl_directory_audits tasks with params: {self.config.model_dump()}")

        tasks = []
        if self.config.user_principal_names:
            tasks = []
            for user_principal_name in self.config.user_principal_names:
                self.logger.debug(f"Create crawl_directory_audits tasks for user: {user_principal_name}")
                tasks.append(
                    TaskWrapper(
                        name=f"crawl_directory_audits_{user_principal_name}",
                        coroutine=self.crawl_directory_audits(user_principal_name),
                    )
                )
        else:
            self.logger.debug("Create crawl_directory_audits task")
            tasks.append(
                TaskWrapper(
                    name="crawl_directory_audits",
                    coroutine=self.crawl_directory_audits(),
                )
            )

        return tasks

    @require_permissions([(ServicePrincipalType.GRAPH_API, "AuditLog.Read.All")])
    async def crawl_directory_audits(self, user_principal_name: str | None = None, split_days: bool = True) -> None:

        date_start, date_end = self._read_date_fields()

        self.logger.info(
            f"Get audit logs from {date_start} to {date_end}"
            + (f" for user {user_principal_name}" if user_principal_name else "")
        )

        custom_filter = await self._build_custom_filter(user_principal_name)

        self.logger.debug(f"crawl_directory_audits with filter {custom_filter}")

        output_filename_prefix = self.create_search_identifier("directory_audits", user_principal_name)

        await self.simple_graph_query(
            filter_timstamp_name="ActivityDateTime",
            output_filename_prefix=output_filename_prefix,
            request_func="audit_logs.directory_audits",
            date_start=date_start,
            date_end=date_end,
            split_days=split_days,
            custom_filter=custom_filter,
            number_interval_days=self.config.number_interval_days,
        )

    async def _get_user_id(self, user_principal_name: str) -> int | None:
        try:
            res = await self.make_graph_request_with_retry(
                request_func=self.graph_client.users.by_user_id(user_principal_name).get
            )

            self.logger.debug(f"User '{user_principal_name}' has ID '{res.id}'")

            return res.id

        except Exception as e:
            self.logger.warning(
                f"Could not get user id for user with principal name '{user_principal_name}'. "
                "Continue with a filter for only the principal name. This will not output all audit events for this user."
            )
            self.logger.debug(e)

        return None
