#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -------------------------------------------------- #
# METADATA                                           #
# -------------------------------------------------- #
__author__ = "Alexander Goedeke"
__version__ = "0.5.1"


# -------------------------------------------------- #
# IMPORTS                                            #
# -------------------------------------------------- #
import json
import os
import re
from datetime import datetime
from typing import Any, List

from ..helpers.permissions import ServicePrincipalType, require_permissions
from ..helpers.pwsh import build_cmdlet_root_model, build_pwsh_request_builder
from ..helpers.registry import register_crawler
from ..helpers.utils import TaskWrapper, custom_serializer, write_json_to_file
from ..interfaces.crawler import BaseCrawler


@register_crawler(name="m365_mailbox_permissions_pwsh")
class MailboxPermissionsPWSHCrawler(BaseCrawler):

    OUTPUT_FILE_SUFFIX = ".jsonl"
    DEFAULT_SCOPES = ["https://outlook.office365.com/.default"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> List[TaskWrapper]:
        self.logger.debug(f"Create crawl_mailbox_permissions pwsh task with params: {self.config.model_dump()}")
        return [TaskWrapper(name="crawl_mailbox_permissions_pwsh", coroutine=self.crawl_mailbox_permissions())]

    @require_permissions(
        [
            (ServicePrincipalType.O365_EXCHANGE, "Exchange.ManageAsApp"),
            (ServicePrincipalType.GRAPH_API, "MailboxSettings.Read"),
            (ServicePrincipalType.GRAPH_API, "User.Read.All"),
        ]
    )
    async def crawl_mailbox_permissions(self):
        await self.ensure_graph_client(scopes=self.DEFAULT_SCOPES)
        if self.graph_client is None:
            return

        configured_user_principal_names = getattr(self.config, "user_principal_names", None) or []
        default_user_principal_names = getattr(self.settings.defaults, "user_principal_names", None) or []
        selected_user_principal_names = []
        selection_identifier = "all_mailboxes"

        if configured_user_principal_names:
            selected_user_principal_names = [str(user_principal_name) for user_principal_name in configured_user_principal_names]
            selection_identifier = self._build_mailbox_selection_identifier(selected_user_principal_names)
            self.logger.info(
                f"Crawling mailbox permissions for {len(selected_user_principal_names)} configured user_principal_names"
            )
        elif default_user_principal_names:
            selected_user_principal_names = [str(user_principal_name) for user_principal_name in default_user_principal_names]
            selection_identifier = self._build_mailbox_selection_identifier(selected_user_principal_names)
            self.logger.info(
                f"Crawling mailbox permissions for {len(selected_user_principal_names)} defaults.user_principal_names entries"
            )

        mailbox_settings_entries = await self._fetch_mailbox_settings_entries_from_graph(selected_user_principal_names)

        target_mailbox_identities = selected_user_principal_names
        if not target_mailbox_identities:
            target_mailbox_identities = await self._get_all_mailbox_identities()
            self.logger.debug(f"Resolved {len(target_mailbox_identities)} mailboxes via Get-Mailbox")

        if not target_mailbox_identities:
            self.logger.warning("No mailboxes found to crawl.")
            return

        mailbox_settings_by_identity = {
            self._get_identity_from_mailbox_settings_entry(entry).lower(): entry
            for entry in mailbox_settings_entries
            if self._get_identity_from_mailbox_settings_entry(entry)
        }

        # Ensure every target mailbox appears in output, even if Graph mailbox settings are missing.
        mailbox_settings_entries = []
        for mailbox_identity in target_mailbox_identities:
            mailbox_settings_entries.append(
                mailbox_settings_by_identity.get(
                    mailbox_identity.lower(),
                    {
                        "UserPrincipalName": mailbox_identity,
                        "mailbox_settings": None,
                    },
                )
            )

        results = []
        for mailbox_settings_entry in mailbox_settings_entries:
            mailbox_identity = self._get_identity_from_mailbox_settings_entry(mailbox_settings_entry)
            if not mailbox_identity:
                continue

            try:
                permission_entry = await self._crawl_single_mailbox_permissions(mailbox_identity)

                merged_entry = self._to_plain_value(mailbox_settings_entry) or {}
                merged_entry["MailboxIdentity"] = mailbox_identity
                merged_entry["Permissions"] = permission_entry
                results.append(merged_entry)
            except Exception as e:
                self.logger.error(f"Failed to crawl mailbox permissions for '{mailbox_identity}': {e}")

        if not results:
            self.logger.info("No mailbox permission data to write.")
            return

        output_file_path = self._build_output_file_path(selection_identifier)

        if os.path.exists(output_file_path):
            self.logger.info(f"Output file '{output_file_path}' already exists. Skipping.")
            return

        try:
            with open(output_file_path, "w", encoding="utf-8") as file:
                for entry in results:
                    write_json_to_file(json_input=entry, file=file)
            self.logger.info(f"Wrote {len(results)} mailbox permission entries to {output_file_path}")
        except Exception as e:
            self.logger.error(f"Failed to write mailbox permissions file: {e}")

    async def _get_all_mailbox_identities(self) -> List[str]:
        mailbox_records = await self._run_pwsh_cmdlet("Get-Mailbox", {"ResultSize": "Unlimited"})
        mailbox_identities = []

        for record in mailbox_records:
            identity = self._get_first_value(
                record,
                "user_principal_name",
                "UserPrincipalName",
                "primary_smtp_address",
                "PrimarySmtpAddress",
                "identity",
                "Identity",
                "alias",
                "Alias",
            )
            if identity and not self._is_system_principal(identity):
                mailbox_identities.append(str(identity))

        return mailbox_identities

    async def _crawl_single_mailbox_permissions(self, mailbox_identity: str) -> dict[str, Any]:
        mailbox_response = await self._run_pwsh_cmdlet("Get-Mailbox", {"Identity": mailbox_identity})
        mailbox_record = mailbox_response[0] if mailbox_response else None

        full_access_permissions = await self._run_pwsh_cmdlet("Get-MailboxPermission", {"Identity": mailbox_identity})
        send_as_permissions = await self._run_pwsh_cmdlet("Get-RecipientPermission", {"Identity": mailbox_identity})

        send_on_behalf = self._extract_send_on_behalf(mailbox_record)

        full_access_permissions = [
            self._to_plain_value(record)
            for record in full_access_permissions
            if not self._is_system_principal(self._get_permission_principal(record))
        ]

        send_as_permissions = [
            self._to_plain_value(record)
            for record in send_as_permissions
            if not self._is_system_principal(self._get_permission_principal(record))
        ]

        send_on_behalf = [principal for principal in send_on_behalf if not self._is_system_principal(principal)]

        return {
            "FullAccess": full_access_permissions,
            "SendAs": send_as_permissions,
            "SendOnBehalf": send_on_behalf,
        }

    async def _fetch_mailbox_settings_entries_from_graph(self, user_principal_names: List[str]) -> List[dict[str, Any]]:
        self.logger.debug("Fetching mailbox settings directly from Graph API.")
        graph_client = await self._create_graph_client(self.settings.auth, scopes=["https://graph.microsoft.com/.default"])
        if graph_client is None:
            return []

        try:
            normalized_filter = {upn.lower() for upn in user_principal_names}
            entries: List[dict[str, Any]] = []

            users = await self.make_graph_request_with_retry_and_pagination(
                output_file_path=None,
                request_func=graph_client.users,
            )

            for user in users:
                user_id = getattr(user, "id", None)
                user_principal_name = getattr(user, "user_principal_name", None)

                if not user_id or not user_principal_name:
                    continue

                if normalized_filter and user_principal_name.lower() not in normalized_filter:
                    continue

                try:
                    mailbox_settings = await self.make_graph_request_with_retry(
                        request_func=graph_client.users.by_user_id(user_id).mailbox_settings.get,
                    )
                except Exception:
                    continue

                if mailbox_settings is None:
                    continue

                entries.append(
                    {
                        "UserId": user_id,
                        "UserPrincipalName": user_principal_name,
                        "mailbox_settings": self._to_plain_value(mailbox_settings),
                    }
                )

            return entries
        finally:
            try:
                await graph_client.request_adapter.close()
            except Exception:
                pass

    def _get_identity_from_mailbox_settings_entry(self, entry: dict[str, Any]) -> str | None:
        identity = self._get_first_value(
            entry,
            "UserPrincipalName",
            "user_principal_name",
            "primary_smtp_address",
            "PrimarySmtpAddress",
            "identity",
            "Identity",
        )

        if identity is None:
            return None

        return str(identity)

    async def _run_pwsh_cmdlet(self, cmdlet_name: str, parameters: dict[str, Any] | None = None) -> List[Any]:
        if parameters is None:
            parameters = {}

        response = await self.make_graph_request_with_retry_and_pagination(
            output_file_path=None,
            request_func=build_pwsh_request_builder(
                request_adapter=self.graph_client.request_adapter,
                tenant_id=self.settings.auth.tenant_id,
            ),
            http_method="POST",
            fail=True,
            body=build_cmdlet_root_model(cmdlet_name=cmdlet_name, parameters=parameters),
        )

        if response is None:
            return []

        if isinstance(response, list):
            return response

        return [response]

    def _build_mailbox_selection_identifier(self, mailbox_identities: List[str]) -> str:
        if not mailbox_identities:
            return "all_mailboxes"

        normalized = []
        for identity in mailbox_identities:
            local_part = identity.split("@")[0]
            safe_value = re.sub(r"[^a-zA-Z0-9._-]", "-", local_part)
            if safe_value:
                normalized.append(safe_value)

        if not normalized:
            return "selected_mailboxes"

        max_entries = 5
        suffix = ""
        if len(normalized) > max_entries:
            suffix = f"_plus-{len(normalized) - max_entries}"

        return self.create_search_identifier(normalized[:max_entries]) + suffix

    def _build_output_file_path(self, selection_identifier: str) -> str:
        day_identifier = datetime.now().strftime("%Y-%m-%d")
        output_filename = f"mailbox_permissions_{selection_identifier}_{day_identifier}{self.OUTPUT_FILE_SUFFIX}"
        return os.path.join(self.output_dir, output_filename)

    def _extract_send_on_behalf(self, mailbox_record: Any) -> List[Any]:
        if mailbox_record is None:
            return []

        candidates = self._get_first_value(
            mailbox_record,
            "grant_send_on_behalf_to",
            "GrantSendOnBehalfTo",
            "grantSendOnBehalfTo",
        )

        if candidates is None:
            return []

        if not isinstance(candidates, list):
            candidates = [candidates]

        principals = []
        for candidate in candidates:
            principal = self._get_permission_principal(candidate)
            if principal:
                principals.append(self._to_plain_value(candidate))

        return principals

    def _get_permission_principal(self, record: Any) -> str | None:
        return self._get_first_value(
            record,
            "user",
            "User",
            "trustee",
            "Trustee",
            "name",
            "Name",
            "display_name",
            "DisplayName",
            "identity",
            "Identity",
            "primary_smtp_address",
            "PrimarySmtpAddress",
            "user_principal_name",
            "UserPrincipalName",
            "email_address",
            "EmailAddress",
            "principal_name",
            "PrincipalName",
        )

    def _get_first_value(self, record: Any, *field_names: str) -> Any:
        if record is None:
            return None

        if isinstance(record, dict):
            for field_name in field_names:
                if field_name in record and record[field_name] not in (None, "", [], {}):
                    return record[field_name]
            return None

        for field_name in field_names:
            if hasattr(record, field_name):
                value = getattr(record, field_name)
                if value not in (None, "", [], {}):
                    return value

        return None

    def _is_system_principal(self, value: Any) -> bool:
        if value is None:
            return True

        if isinstance(value, dict):
            return any(self._is_system_principal(nested_value) for nested_value in value.values())

        if isinstance(value, list):
            return all(self._is_system_principal(item) for item in value)

        text = str(value).strip().lower()
        system_patterns = [
            r"^nt authority\\",
            r"^s-1-5-",
            r"^anonymous$",
            r"^default$",
            r"^self$",
            r"^system$",
            r"^exchange trusted subsystem$",
            r"^healthmailbox",
            r"^discoverysearchmailbox",
            r"^systemmailbox",
            r"^federatedemail\.",
            r"^migration\.",
            r"^msol_",
            r"^security principals?",
        ]
        return any(re.search(pattern, text) for pattern in system_patterns)

    def _to_plain_value(self, value: Any) -> Any:
        if value is None:
            return None

        return json.loads(json.dumps(value, ensure_ascii=True, default=custom_serializer))