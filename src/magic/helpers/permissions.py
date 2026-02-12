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
import functools
import json
import requests
from .config import Settings
from typing import List
from pathlib import Path
from enum import Enum
from uuid import UUID
from collections import defaultdict
from msgraph_beta import GraphServiceClient
from ..helpers.logging import Logger
from ..helpers.mixins import CreateGraphClientMixin
from msgraph_beta.generated.models.service_principal import ServicePrincipal
from msgraph_beta.generated.service_principals_with_app_id.service_principals_with_app_id_request_builder import (
    ServicePrincipalsWithAppIdRequestBuilder,
)
from msgraph_beta.generated.applications.applications_request_builder import ApplicationsRequestBuilder


class ServicePrincipalType(Enum):
    GRAPH_API = UUID("00000003-0000-0000-c000-000000000000")
    O365_EXCHANGE = UUID("00000002-0000-0ff1-ce00-000000000000")
    AZURE_CLI = UUID("14d82eec-204b-4c2f-b7e8-296a70dab67e")

    @property
    def app_id(self) -> str:
        return str(self.value)


class PermissionValidator(CreateGraphClientMixin):

    SERVICE_PRINCIPAL_REQUEST_CONFIGURATION: (
        ServicePrincipalsWithAppIdRequestBuilder.ServicePrincipalsWithAppIdRequestBuilderGetRequestConfiguration
    )

    graph_client: GraphServiceClient

    settings: Settings

    permissions_to_grant: List[str | None]

    def __init__(self, settings: Settings, reports_dir: str, permissions_to_grant: set = set(), debug: bool = False):
        self.settings = settings
        self.debug = debug

        self.SERVICE_PRINCIPAL_REQUEST_CONFIGURATION = ServicePrincipalsWithAppIdRequestBuilder.ServicePrincipalsWithAppIdRequestBuilderGetRequestConfiguration(
            query_parameters=ServicePrincipalsWithAppIdRequestBuilder.ServicePrincipalsWithAppIdRequestBuilderGetQueryParameters(
                select=[
                    'id',
                    'appId',
                    'displayName',
                    'appRoles',
                    'oauth2PermissionScopes',
                    'resourceSpecificApplicationPermissions',
                    'requiredResourceAccess',
                ]
            )
        )

        """ init permissions to grant already grouped """
        self.permissions_to_grant = defaultdict(list)
        for principal, permission in permissions_to_grant:
            self.permissions_to_grant[principal].append(permission)

        logger = Logger(__name__, reports_dir, debug)
        self.logger = logger.bootstrap()

    async def _get_service_principal(
        self,
        app_id: str,
    ) -> ServicePrincipal:
        self.logger.debug(f"Get service principal for {app_id}")

        service_principal = await self.graph_client.service_principals_with_app_id(app_id).get(
            request_configuration=self.SERVICE_PRINCIPAL_REQUEST_CONFIGURATION
        )
        return service_principal

    async def _get_app_service_principal(self) -> ServicePrincipal:
        self.logger.debug(f"Get service principal for client id {self.settings.auth.client_id}")
        return await self._get_service_principal(self.settings.auth.client_id)

    async def _get_application(self):
        request_configuration = ApplicationsRequestBuilder.ApplicationsRequestBuilderGetRequestConfiguration(
            query_parameters=ApplicationsRequestBuilder.ApplicationsRequestBuilderGetQueryParameters(
                filter=f"appId eq '{self.settings.auth.client_id}'"
            )
        )

        res = await self.graph_client.applications.get(request_configuration=request_configuration)
        application = res.value[0]

        return application

    async def create_manifest(self):
        manifest = {"requiredResourceAccess": []}

        proxy_base_url = "https://graphexplorer.microsoft.com/api/proxy"

        headers = {
            "Authorization": "Bearer {token:https://graph.microsoft.com/}",
            "Content-Type": "application/json",
            "Accept-Language": "de-DE,de;q=0.9",
            "Prefer": "ms-graph-dev-mode",
            "Origin": "https://developer.microsoft.com",
            "Referer": "https://developer.microsoft.com/",
        }

        for service_principal_type, permissions in self.permissions_to_grant.items():

            """get service principal by app id via graph proxy"""
            try:
                params = {
                    "url": f"https://graph.microsoft.com/beta/servicePrincipals?$filter=appId eq '{service_principal_type.app_id}'"
                }

                res = requests.get(proxy_base_url, headers=headers, params=params)
                res.raise_for_status()

                data = res.json()

                service_principal = data['value'][0]
            except Exception as e:
                self.logger.error(e)
                continue

            resource_access = []
            for permission in permissions:
                """find correlating app_role"""
                app_role = next(filter(lambda item: item['value'] == permission, service_principal['appRoles']), None)

                if app_role:
                    resource_access.append({"id": str(app_role['id']), "type": 'Role'})

            manifest["requiredResourceAccess"].append(
                {"resourceAppId": service_principal_type.app_id, "resourceAccess": resource_access}
            )

        """ save permission manifest to file """
        output_file_path = Path("permission_manifest.json")

        self.logger.debug(f"Saving permission manifest to {output_file_path}")

        with open(output_file_path, "w", encoding="utf-8") as permission_manifest_file:
            json.dump(manifest, permission_manifest_file, indent=4, ensure_ascii=False)

        self.logger.info(f"Finished generating permission manifest to file {output_file_path}")

    async def validate(self) -> bool:

        self.logger.info("Starting with the permission check of the configured application.")
        self.graph_client = await self._create_graph_client(self.settings.auth)
        if self.graph_client is None:
            return False

        validation = True

        app_service_principal = await self._get_app_service_principal()
        if app_service_principal is None:
            return False

        try:
            current_app_role_assignments = await self.graph_client.service_principals.by_service_principal_id(
                app_service_principal.id
            ).app_role_assignments.get()
        except Exception as e:
            self.logger.error(
                f"Could not get the service principals from the graph api. There is probably something wrong with the provided credentials. {e}"
            )
            return False

        self.logger.debug(f"Checking {sum(len(perms) for perms in self.permissions_to_grant.values())} permissions")

        for service_principal_type, permissions in self.permissions_to_grant.items():
            service_principal = await self._get_service_principal(service_principal_type.app_id)

            self.logger.debug(f"Checking {service_principal.display_name} permissions:")

            for app_role in service_principal.app_roles:
                try:
                    if app_role.value not in permissions:
                        continue

                    if app_role.id in [
                        app_role_assignment.app_role_id for app_role_assignment in current_app_role_assignments.value
                    ]:
                        self.logger.debug(f"    {app_role.value} -> exists - satisfied.")
                        continue

                    self.logger.debug(f"    {app_role.value} -> not existing - unsatisfied.")
                    validation = False
                except Exception as e:
                    self.logger.debug(f"    {app_role.value} -> error ocured.")
                    self.logger.error(e)
                    return False

        return validation


def require_permissions(permissions: List[tuple]):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):

            if hasattr(self, "register_required_permissions"):
                self.register_required_permissions(permissions)

            return func(self, *args, **kwargs)

        return wrapper

    return decorator
