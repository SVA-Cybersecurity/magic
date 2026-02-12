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
from msgraph_beta import GraphServiceClient
from azure.identity.aio import ClientSecretCredential
from .config import AuthSettings
from azure.core.exceptions import ClientAuthenticationError


class CreateGraphClientMixin:

    async def _create_graph_client(
        self, auth: AuthSettings, scopes: List[str] = ["https://graph.microsoft.com/.default"]
    ) -> None:
        credentials = None
        try:
            credentials = ClientSecretCredential(
                auth.tenant_id,
                auth.client_id,
                auth.client_secret,
            )

            # check if token is correct
            await credentials.get_token(scopes[0])

            return GraphServiceClient(credentials=credentials, scopes=scopes)
        except ClientAuthenticationError as e1:
            self.logger.error(f"Invalid authentication configuration. {e1.message}")
            if credentials:
                await credentials.close()
            return None
        except Exception as e2:
            self.logger.error(
                f"Something went wrong while init the graph service client. There is probably something wrong with the provided credentials. {e2}"
            )
            if credentials:
                await credentials.close()
            return None
