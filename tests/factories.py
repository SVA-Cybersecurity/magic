#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test data factories for magic config models and common test objects.
"""

from magic.helpers.config import (
    M365UALConfig,
    M365SigninConfig,
    M365AuditConfig,
    M365MessageTracesConfig,
    M365MessageTracesPWSHConfig,
    M365MessagesConfig,
    M365MessageConfig,
    M365Config,
    Settings,
    AuthSettings,
    Defaults,
    EnrichConfig,
)


def make_ual_config(**overrides) -> M365UALConfig:
    """Create a valid M365UALConfig with optional overrides."""
    defaults = {
        "type": "m365_ual",
    }
    return M365UALConfig(**{**defaults, **overrides})


def make_signin_config(**overrides) -> M365SigninConfig:
    """Create a valid M365SigninConfig with optional overrides."""
    defaults = {
        "type": "m365_signin",
    }
    return M365SigninConfig(**{**defaults, **overrides})


def make_audit_config(**overrides) -> M365AuditConfig:
    """Create a valid M365AuditConfig with optional overrides."""
    defaults = {
        "type": "m365_audit",
    }
    return M365AuditConfig(**{**defaults, **overrides})


def make_message_traces_config(**overrides) -> M365MessageTracesConfig:
    """Create a valid M365MessageTracesConfig with optional overrides."""
    defaults = {
        "type": "m365_message_traces",
    }
    return M365MessageTracesConfig(**{**defaults, **overrides})


def make_message_traces_pwsh_config(**overrides) -> M365MessageTracesPWSHConfig:
    """Create a valid M365MessageTracesPWSHConfig with optional overrides."""
    defaults = {
        "type": "m365_message_traces_pwsh",
        "sender_addresses": ["test@example.com"],
    }
    return M365MessageTracesPWSHConfig(**{**defaults, **overrides})


def make_messages_config(**overrides) -> M365MessagesConfig:
    """Create a valid M365MessagesConfig with optional overrides."""
    defaults = {
        "type": "m365_messages",
        "user_principal_names": ["test@example.com"],
    }
    return M365MessagesConfig(**{**defaults, **overrides})


def make_message_config(**overrides) -> M365MessageConfig:
    """Create a valid M365MessageConfig with optional overrides."""
    defaults = {
        "type": "m365_message",
        "user_principal_name": "test@example.com",
        "message_id": "test-message-id-123",
    }
    return M365MessageConfig(**{**defaults, **overrides})


def make_m365_config(**overrides) -> M365Config:
    """Create a valid M365Config with all flags defaulting to False."""
    defaults = {f: False for f in M365Config.model_fields if f != "type"}
    defaults["type"] = "m365"
    return M365Config(**{**defaults, **overrides})


def make_auth_settings(**overrides) -> AuthSettings:
    """Create a valid AuthSettings with optional overrides."""
    defaults = {
        "client_secret": "test-secret",
        "client_id": "test-client-id",
        "tenant_id": "test-tenant-id",
    }
    return AuthSettings(**{**defaults, **overrides})


def make_defaults(**overrides) -> Defaults:
    """Create a valid Defaults with optional overrides."""
    defaults = {
        "user_principal_names": None,
        "date_start": None,
        "date_end": None,
    }
    return Defaults(**{**defaults, **overrides})


def make_settings(**overrides) -> Settings:
    """Create a valid Settings with optional overrides."""
    defaults = {
        "permission_preflight_check": False,
        "auth": make_auth_settings(),
        "defaults": make_defaults(),
    }
    return Settings(**{**defaults, **overrides})


def make_enrich_config(**overrides) -> EnrichConfig:
    """Create a valid EnrichConfig with optional overrides."""
    defaults = {
        "timesketch": {"enabled": False, "output_filename": "ts.jsonl"},
        "ipapi": {"enabled": False, "output_filename": "ipapi.jsonl"},
        "hash": {
            "enabled": False,
            "output_filename": "hash.jsonl",
            "output_filename_csv": "hash.csv",
        },
    }
    return EnrichConfig(**{**defaults, **overrides})


def make_crawler_kwargs(tmp_path, **overrides) -> dict:
    """Create common kwargs for any BaseCrawler constructor.

    Requires a ``tmp_path`` (pytest built-in) so that ``reports_dir`` and
    ``output_dir`` point to valid temporary directories.
    """
    output_dir = tmp_path / "output"
    output_dir.mkdir(exist_ok=True)
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir(exist_ok=True)

    defaults = {
        "reports_dir": str(logs_dir),
        "settings": make_settings(),
        "output_dir": str(output_dir),
        "config": None,
        "debug": False,
    }
    return {**defaults, **overrides}
