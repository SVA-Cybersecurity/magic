#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Shared pytest fixtures available to all test modules.

Uses factory functions from tests/factories.py to keep fixture
definitions thin and consistent.
"""

import pytest
from unittest.mock import MagicMock
from tests.factories import (
    make_settings,
    make_enrich_config,
    make_audit_config,
    make_ual_config,
    make_signin_config,
    make_message_traces_config,
    make_message_traces_pwsh_config,
    make_messages_config,
    make_message_config,
    make_m365_config,
    make_crawler_kwargs,
)


@pytest.fixture
def mock_logger():
    """Provide a MagicMock that satisfies the Logger interface."""
    return MagicMock()


@pytest.fixture
def settings():
    """Valid Settings instance with test defaults."""
    return make_settings()


@pytest.fixture
def enrich_config():
    """Valid EnrichConfig instance with test defaults."""
    return make_enrich_config()


@pytest.fixture
def ual_config():
    """Valid M365UALConfig instance."""
    return make_ual_config()


@pytest.fixture
def signin_config():
    """Valid M365SigninConfig instance."""
    return make_signin_config()


@pytest.fixture
def audit_config():
    """Valid M365AuditConfig instance."""
    return make_audit_config()


@pytest.fixture
def message_traces_config():
    """Valid M365MessageTracesConfig instance."""
    return make_message_traces_config()


@pytest.fixture
def message_traces_pwsh_config():
    """Valid M365MessageTracesPWSHConfig instance."""
    return make_message_traces_pwsh_config()


@pytest.fixture
def messages_config():
    """Valid M365MessagesConfig instance."""
    return make_messages_config()


@pytest.fixture
def message_config():
    """Valid M365MessageConfig instance."""
    return make_message_config()


@pytest.fixture
def m365_config():
    """Valid M365Config instance with all flags set to False."""
    return make_m365_config()


@pytest.fixture
def crawler_kwargs(tmp_path):
    """Common kwargs dict for any BaseCrawler constructor."""
    return make_crawler_kwargs(tmp_path)
