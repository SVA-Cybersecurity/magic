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
from typing import List, Optional, Tuple
from msgraph_beta import GraphServiceClient
from azure.identity.aio import ClientSecretCredential
from .config import AuthSettings
from azure.core.exceptions import ClientAuthenticationError

DEFAULT_GRAPH_SCOPES: Tuple[str, ...] = ("https://graph.microsoft.com/.default",)


async def create_validated_credential(
    auth: AuthSettings, logger, validation_scope: str = DEFAULT_GRAPH_SCOPES[0]
) -> ClientSecretCredential | None:
    credential = ClientSecretCredential(
        auth.tenant_id,
        auth.client_id,
        auth.client_secret,
    )
    try:
        # opens the credential's aiohttp session and verifies the app secret
        await credential.get_token(validation_scope)
        return credential
    except ClientAuthenticationError as e1:
        logger.error(f"Invalid authentication configuration. {e1.message}")
        await credential.close()
        return None
    except Exception as e2:
        logger.error(
            f"Something went wrong while validating the credentials. There is probably something wrong with the provided credentials. {e2}"
        )
        await credential.close()
        return None


class CreateGraphClientMixin:

    def _build_graph_client(self, scopes: Optional[List[str]] = None) -> GraphServiceClient | None:
        credential = getattr(self, "_credential", None)
        if credential is None:
            self.logger.error("No credential was injected - cannot build a graph client.")
            return None

        scopes = list(scopes) if scopes else list(DEFAULT_GRAPH_SCOPES)
        return GraphServiceClient(credentials=credential, scopes=scopes)

    async def _close_graph_client(self, client: GraphServiceClient | None) -> None:
        if client is None:
            return
        try:
            adapter = getattr(client, "request_adapter", None)
            http_client = getattr(adapter, "_http_client", None) if adapter is not None else None
            if http_client is not None and hasattr(http_client, "aclose"):
                await http_client.aclose()
        except Exception as e:
            self.logger.debug(f"Error closing graph client HTTP client: {e}")