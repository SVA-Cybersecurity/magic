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
from ..helpers.permissions import require_permissions, ServicePrincipalType


@register_crawler(name="m365")
class M365Crawler(BaseCrawler):

    task_prefix: str = "crawl"

    def __init__(self, **kwargs):
        super().__init__(**kwargs, logger=__name__)

    def get_tasks(self) -> List[TaskWrapper]:
        tasks = []

        for key, val in self.config.items():
            try:
                func = getattr(self, f"{self.task_prefix}_{str(key)}")
                if func is None:
                    raise Exception
            except Exception:
                self.logger.warning(f"Did not find '{key}' in crawler")
                continue

            if val:
                tasks.append(TaskWrapper(name=func.__name__, coroutine=func()))
        return tasks

    @require_permissions(
        [
            (ServicePrincipalType.GRAPH_API, "User.Read"),
            (ServicePrincipalType.GRAPH_API, "Directory.Read.All"),
            (ServicePrincipalType.GRAPH_API, "Group.Read.All"),
            (ServicePrincipalType.GRAPH_API, "GroupMember.Read.All"),
            (ServicePrincipalType.GRAPH_API, "User.Read.All"),
        ]
    )
    async def crawl_users_transitive_member_of(self):
        await self.make_graph_request_for_child_items(
            parent="users",
            child="transitive_member_of",
            output_filename="users_transitive_member_of",
            identifier_function="by_user_id",
            parent_fields_to_child={
                "UserId": "id",
                "UserPrincipalName": "user_principal_name",
            },
        )

    @require_permissions([(ServicePrincipalType.GRAPH_API, "Mail.Read")])
    async def crawl_message_rules(self):
        await self.make_graph_request_for_child_items(
            parent="users",
            child="mail_folders.by_mail_folder_id('inbox').message_rules",
            output_filename="message_rules",
            identifier_function="by_user_id",
            parent_fields_to_child={
                "UserId": "id",
                "UserPrincipalName": "user_principal_name",
            },
        )

    @require_permissions([(ServicePrincipalType.GRAPH_API, "MailboxSettings.Read")])
    async def crawl_mailbox_settings(self):
        await self.make_graph_request_for_child_items(
            parent="users",
            child="mailbox_settings",
            output_filename="mailbox_settings",
            identifier_function="by_user_id",
            parent_fields_to_child={
                "UserId": "id",
                "UserPrincipalName": "user_principal_name",
            },
        )

    @require_permissions(
        [
            (ServicePrincipalType.GRAPH_API, "UserAuthenticationMethod.Read.All"),
            (ServicePrincipalType.GRAPH_API, "UserAuthenticationMethod.Read"),
            (ServicePrincipalType.GRAPH_API, "AuditLog.Read.All"),
        ]
    )
    async def crawl_authentication_methods(self):
        await self.make_graph_request_for_child_items(
            parent="users",
            child="authentication.methods",
            output_filename="authentication_methods",
            identifier_function="by_user_id",
            parent_fields_to_child={
                "UserId": "id",
                "UserPrincipalName": "user_principal_name",
            },
        )
        await self.simple_graph_query(
            output_filename_prefix="authentication_methods_report",
            request_func="reports.authentication_methods.user_registration_details",
        )

    @require_permissions(
        [
            (ServicePrincipalType.GRAPH_API, "ProvisioningLog.Read.All"),
            (ServicePrincipalType.GRAPH_API, "AuditLog.Read.All"),
            (ServicePrincipalType.GRAPH_API, "Directory.Read.All"),
        ]
    )
    async def crawl_directory_provisioning(self):
        await self.simple_graph_query(
            output_filename_prefix="directory_provisioning",
            request_func="audit_logs.provisioning",
        )

    @require_permissions(
        [
            (ServicePrincipalType.GRAPH_API, "RoleManagement.Read.Directory"),
            (ServicePrincipalType.GRAPH_API, "Directory.Read.All"),
        ]
    )
    async def crawl_directory_roles(self):
        await self.simple_graph_query(output_filename_prefix="directory_roles", request_func="directory_roles")

    @require_permissions(["IdentityRiskyServicePrincipal.Read.All", "IdentityRiskyUser.Read.All"])
    async def crawl_risk_detections(self):
        await self.simple_graph_query(
            output_filename_prefix="risk_detections",
            request_func=[
                "identity_protection.risk_detections",
                "identity_protection.service_principal_risk_detections",
            ],
        )

    @require_permissions(
        [
            (ServicePrincipalType.GRAPH_API, "IdentityRiskyServicePrincipal.Read.All"),
            (ServicePrincipalType.GRAPH_API, "IdentityRiskyUser.Read.All"),
        ]
    )
    async def crawl_risky_users(self):
        await self.simple_graph_query(
            output_filename_prefix="risky_users",
            request_func=[
                "identity_protection.risky_users",
                "identity_protection.risky_service_principals",
            ],
        )

        await self.make_graph_request_for_child_items(
            parent="risky_users",
            child="history",
            identifier_function="by_risky_user_id",
        )

        await self.make_graph_request_for_child_items(
            parent="identity_protection.risky_service_principals",
            child="history",
            identifier_function="by_risky_service_principal_id",
        )

    @require_permissions(
        [
            (ServicePrincipalType.GRAPH_API, "Application.Read.All"),
            (ServicePrincipalType.GRAPH_API, "Directory.Read.All"),
            (ServicePrincipalType.GRAPH_API, "ConsentRequest.Read.All"),
            (ServicePrincipalType.GRAPH_API, "Policy.Read.PermissionGrant"),
        ]
    )
    async def crawl_applications(self):
        default_parent_fields_to_child = {
            "ApplicationId": "id",
            "ApplicationDisplayName": "display_name",
        }

        await self.simple_graph_query(
            output_filename_prefix="applications",
            request_func=[
                "applications",
                "directory.deleted_items.graph_application",
                "identity_governance.app_consent.app_consent_requests",
            ],
        )

        await self.make_graph_request_for_child_items(
            parent="applications",
            child="extension_properties",
            identifier_function="by_application_id",
            parent_fields_to_child=default_parent_fields_to_child,
        )

        await self.make_graph_request_for_child_items(
            parent="applications",
            child="federated_identity_credentials",
            identifier_function="by_application_id",
            parent_fields_to_child=default_parent_fields_to_child,
        )

        await self.make_graph_request_for_child_items(
            parent="applications",
            child="owners",
            identifier_function="by_application_id",
            parent_fields_to_child=default_parent_fields_to_child,
        )

        await self.make_graph_request_for_child_items(
            parent="applications",
            child="token_lifetime_policies",
            identifier_function="by_application_id",
            parent_fields_to_child=default_parent_fields_to_child,
        )

        await self.make_graph_request_for_child_items(
            parent="applications",
            child="token_issuance_policies",
            identifier_function="by_application_id",
            parent_fields_to_child=default_parent_fields_to_child,
        )

        await self.simple_graph_query(
            output_filename_prefix="permission_grant_policies",
            request_func=["policies.permission_grant_policies"],
        )

    @require_permissions([(ServicePrincipalType.GRAPH_API, "Policy.Read.All")])
    async def crawl_conditional_access(self):
        await self.simple_graph_query(
            output_filename_prefix="conditional_access",
            request_func=[
                "identity.conditional_access.policies",
                "identity.conditional_access.authentication_strengths.policies",
            ],
        )

    @require_permissions([(ServicePrincipalType.GRAPH_API, "SecurityEvents.Read.All")])
    async def crawl_security(self):
        await self.simple_graph_query(
            output_filename_prefix="alerts",
            request_func=[
                "security.alerts_v2",
                "security.alerts",
            ],
        )

    @require_permissions(
        [(ServicePrincipalType.GRAPH_API, "Application.Read.All"), (ServicePrincipalType.GRAPH_API, "Directory.Read.All")]
    )
    async def crawl_service_principals(self):
        await self.simple_graph_query(
            output_filename_prefix="service_principals",
            request_func=[
                "service_principals",
                "directory.deleted_items.graph_service_principal",
            ],
        )

    @require_permissions(
        [(ServicePrincipalType.GRAPH_API, "Application.Read.All"), (ServicePrincipalType.GRAPH_API, "Directory.Read.All")]
    )
    async def crawl_service_principals_transitive_member_of(self):
        await self.make_graph_request_for_child_items(
            parent="service_principals",
            child="transitive_member_of",
            output_filename="service_principals_transitive_member_of",
            identifier_function="by_service_principal_id",
            parent_fields_to_child={
                "ServicePrincipalId": "id",
                "ServicePrincipalDisplayName": "display_name",
            },
        )

    @require_permissions(
        [
            (ServicePrincipalType.GRAPH_API, "User.ReadBasic.All"),
            (ServicePrincipalType.GRAPH_API, "DeviceManagementApps.Read.All"),
            (ServicePrincipalType.GRAPH_API, "DeviceManagementConfiguration.Read.All"),
            (ServicePrincipalType.GRAPH_API, "DeviceManagementManagedDevices.Read.All"),
            (ServicePrincipalType.GRAPH_API, "DeviceManagementServiceConfig.Read.All"),
            (ServicePrincipalType.GRAPH_API, "Directory.Read.All"),
            (ServicePrincipalType.GRAPH_API, "User.Read.All"),
            (ServicePrincipalType.GRAPH_API, "OrgContact.Read.All"),
        ]
    )
    async def crawl_users(self):
        await self.simple_graph_query(
            output_filename_prefix="users",
            request_func=["users", "contacts", "directory.deleted_items.graph_user"],
        )

    @require_permissions([(ServicePrincipalType.GRAPH_API, "Directory.Read.All")])
    async def crawl_permission_grants(self):
        await self.simple_graph_query(
            output_filename_prefix="permission_grants",
            request_func="oauth2_permission_grants",
        )

    @require_permissions([(ServicePrincipalType.GRAPH_API, "Directory.Read.All")])
    async def crawl_groups(self):
        await self.simple_graph_query(
            output_filename_prefix="groups",
            request_func=["groups", "directory.deleted_items.graph_group"],
        )

    @require_permissions(
        [(ServicePrincipalType.GRAPH_API, "Directory.Read.All"), (ServicePrincipalType.GRAPH_API, "Device.Read.All")]
    )
    async def crawl_devices(self):
        await self.simple_graph_query(
            output_filename_prefix="devices",
            request_func=["devices", "directory.deleted_items.graph_device"],
        )

    @require_permissions([(ServicePrincipalType.GRAPH_API, "DirectoryRecommendations.Read.All")])
    async def crawl_recommendations(self):
        await self.simple_graph_query(
            output_filename_prefix="recommendations",
            request_func="directory.recommendations",
        )

    @require_permissions([(ServicePrincipalType.GRAPH_API, "AttackSimulation.Read.All")])
    async def crawl_attack_simulation(self):
        await self.simple_graph_query(
            output_filename_prefix="attack_simulation",
            request_func=[
                "security.attack_simulation.simulations",
                "security.attack_simulation.trainings",
                "security.attack_simulation.training_campaigns",
            ],
        )
