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
import json
import re
from ..interfaces.enricher import BaseEnricher
from ..helpers.utils import TaskWrapper, write_json_to_file
from ..helpers.registry import register_enricher


@register_enricher(name="timesketch")
class Timesketch(BaseEnricher):

    TIME_FIELDS = [
        "LastModifiedTime",
        "CreatedDateTime",
        "DeletedDateTime",
        "RefreshTokensValidFromDateTime",
        "RenewedDateTime",
        "SignInSessionsValidFromDateTime",
        "StartTime",
        "CreationTime",
        "TokenIssuedAtTime",
        "CreationTimestamp",
        "RiskLastUpdatedDateTime",
        "EventDateTime",
        "ActivityDateTime",
        "ModifiedDateTime",
        "AuthenticationStepDateTime",
        "LastModifiedDateTime",
        "SentDateTime",
        "ReceivedDateTime",
        "Received",
        "LastUpdatedDateTime",
        "LoggedDateTime",
    ]

    MESSAGE_MAPPING = {
        "m365_ual/ual": "{Operation}",
        "m365_signin/sign_ins": "SignIn from '{UserPrincipalName}'",
        "m365_audit/directory_audits": "{ActivityDisplayName}",
        "m365_message_traces/message_traces": "Message trace '{Subject}'",
        "m365_message_traces_pwsh/message_traces": "Message trace '{Subject}'",
        "m365/authentication_methods.json": "Authentication method for user '{UserPrincipalName}'",
        "m365/authentication_methods_report.json": "Default authentication method for user '{UserPrincipalName}' is '{DefaultMfaMethod}'",
        "m365/message_rules.json": "Inbox rule '{DisplayName}' for user '{UserPrincipalName}'",
        "m365/users_transitive_member_of.json": "User '{UserPrincipalName}' is member of group '{DisplayName}'",
        "m365/service_principals_transitive_member_of.json": "Service principal '{ServicePrincipalDisplayName}' "
        "is member of group '{DisplayName}'",
        "m365/mailbox_settings.json": "Mailbox '{UserPrincipalName}'",
        "m365/applications.json": "Application '{DisplayName}'",
        "m365/conditional_access.json": "Conditional access policy '{DisplayName}'",
        "m365/directory_provisioning.json": "Directory provisioning",
        "m365/directory_roles.json": "Directory role '{DisplayName}'",
        "m365/risk_detections.json": "Risky detection '{UserDisplayName}'",
        "m365/risky_users.json": "Risky user '{UserDisplayName}' - '{RiskState}'",
        "m365/risky_users_history.json": "Risky user '{UserDisplayName}' - '{RiskState}'",
        "m365/alerts.json": "Alert '{Title}'",
        "m365/security.json": "Security action with score {MaxScore}",
        "m365/service_principals.json": "Service principal '{DisplayName}'",
        "m365/users.json": "User '{DisplayName}'",
        "m365/groups.json": "Group '{DisplayName}'",
        "m365/devices.json": "Device",
        "m365/permission_grants.json": "Permission grant '{Scope}' to '{ConsentType}'",
        "m365/recommendations.json": "Recommendation '{DisplayName}'",
        "m365/attack_simulation.json": "Attack simulation '{DisplayName}'",
        "m365/applications_federated_identity_credentials.json": "Federated identity credentials'{Name}' "
        "for application '{ApplicationDisplayName}'",
        "m365/applications_owners.json": "Application owner '{UserPrincipalName}' for application '{ApplicationDisplayName}'",
        "m365/applications_extension_properties.json": "Application extension property "
        "for application '{ApplicationDisplayName}'",
        "m365/applications_token_issuance_policies.json": "Application token issuance policy "
        "for application '{ApplicationDisplayName}'",
        "m365/applications_token_lifetime_policies.json": "Application token lifetime policy "
        "for application '{ApplicationDisplayName}'",
        "m365/permission_grant_policies.json": "Permission grant policy '{DisplayName}'",
        "m365_messages/messages_": "E-Mail '{Subject}'",
    }

    DATE_PATTERNS = [
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?(\+\d\d:\d\d)?$",
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$",
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}$",
    ]

    # Compile the regex patterns once
    COMPILED_DATE_PATTERNS = [re.compile(pattern) for pattern in DATE_PATTERNS]

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self):
        if self._is_enabled():
            self.logger.debug(f"Create output_timesketch task with params: {self.config.model_dump()}")

            return [TaskWrapper(name="output_timesketch", coroutine=self.output_timesketch())]
        return []

    def is_valid_timestamp(self, timestamp_str):
        for pattern in self.COMPILED_DATE_PATTERNS:
            if pattern.match(timestamp_str):
                return True
        return False

    def get_message_field_name(self, filename):
        for key in self.MESSAGE_MAPPING.keys():
            if key in filename:
                return self.MESSAGE_MAPPING[key]
        return None

    def can_filename_be_parsed(self, filename):
        for key in self.MESSAGE_MAPPING.keys():
            if key in filename:
                return True
        return False

    async def output_timesketch(self):
        input_file = os.path.join(self.output_dir, self.config.input_filename)
        output_file = os.path.join(self.output_dir, self.config.output_filename)

        if not os.path.exists(input_file):
            self.logger.error(
                f"File {input_file} not found. Please configure the jsonl enricher before processing data to timesketch enrichment!"
            )
            return

        with open(output_file, "w") as out_file:

            self.logger.debug(f"Converting file {input_file} with timesketch output module")

            with open(input_file, "rb") as in_file:
                for line in in_file:
                    try:
                        json_line = json.loads(line)

                        message_field_name = self.get_message_field_name(json_line["path"])

                        if not self.can_filename_be_parsed(json_line["path"]):
                            self.logger.warning(
                                f"No mapping for file with name {json_line['path']} provided. Skipping entries for this file."
                            )
                            continue

                        if isinstance(json_line, dict):

                            """Duplicate events if they have multiple timestamps"""
                            is_written = False
                            for time_field in self.TIME_FIELDS:
                                if time_field in json_line.keys() and self.is_valid_timestamp(json_line[time_field]):

                                    """normalize date field and remove origin"""
                                    json_line["datetime"] = json_line[time_field]
                                    del json_line[time_field]
                                    json_line["timestamp_desc"] = str(time_field)

                                    try:
                                        if message_field_name:
                                            json_line["message"] = message_field_name.format(**json_line)
                                    except Exception as e:
                                        self.logger.warning(
                                            f"Error while formatting the message field from file {input_file} "
                                            f"with the mapping: {e}. Setting message to None and continue"
                                        )
                                    write_json_to_file(json_line, out_file)
                                    is_written = True

                            if not is_written:
                                json_line["datetime"] = "1990-01-01T00:00:00Z"
                                json_line["timestamp_desc"] = ""

                                if message_field_name:
                                    try:
                                        json_line["message"] = message_field_name.format(**json_line)
                                    except Exception as e:
                                        self.logger.warning(
                                            f"Error while formatting the message field from file {input_file} "
                                            f"with the mapping: {e}. Setting message to None and continue"
                                        )

                                write_json_to_file(json_line, out_file)
                        else:
                            raise
                    except Exception:
                        self.logger.warning(f"Invalid JSON line in file {input_file}")
