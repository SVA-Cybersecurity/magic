#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for magic.crawler modules.

Each crawler test class mixes in ``DateFieldTestMixin`` to get
standard coverage for ``_read_date_fields`` and retention defaults.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, patch
from magic.crawler.audit import AuditCrawler
from magic.crawler.signin import SignInCrawler
from magic.crawler.ual import UalCrawler
from magic.crawler.message_traces import MessageTracesCrawler
from magic.crawler.message_traces_pwsh import MessageTracesPWSHCrawler
from magic.crawler.messages import MessagesCrawler
from magic.crawler.message import MessageCrawler
from magic.crawler.m365 import M365Crawler
from magic.helpers.config import (
    RETENTION_AUDIT,
    RETENTION_SIGN_IN,
    RETENTION_UAL,
    RETENTION_MESSAGE_TRACES,
    RETENTION_MESSAGES,
    RETENTION_DEFAULT,
    SignInType,
)
from magic.helpers.utils import TaskWrapper
from tests.factories import (
    make_audit_config,
    make_signin_config,
    make_ual_config,
    make_message_traces_config,
    make_message_traces_pwsh_config,
    make_messages_config,
    make_message_config,
    make_m365_config,
    make_crawler_kwargs,
)
from tests.mixins import DateFieldTestMixin


class TestAuditCrawler(DateFieldTestMixin):
    """Tests for the AuditCrawler."""

    crawler_class = AuditCrawler
    config_factory = staticmethod(make_audit_config)
    expected_retention = RETENTION_AUDIT

    # ------------------------------------------ #
    # get_tasks: no user_principal_names         #
    # ------------------------------------------ #

    def test_get_tasks_no_users_returns_single_task(self, tmp_path):
        """Without user_principal_names, get_tasks should return exactly one TaskWrapper."""
        config = make_audit_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = AuditCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)
        assert tasks[0].name == "crawl_directory_audits"

    # ------------------------------------------ #
    # get_tasks: single user_principal_name      #
    # ------------------------------------------ #

    def test_get_tasks_single_user_returns_one_task(self, tmp_path):
        """With one user_principal_name, get_tasks should return one TaskWrapper."""
        config = make_audit_config(user_principal_names=["user@example.com"])
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = AuditCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)
        assert "user@example.com" in tasks[0].name

    # ------------------------------------------ #
    # get_tasks: multiple user_principal_names   #
    # ------------------------------------------ #

    def test_get_tasks_multiple_users_returns_task_per_user(self, tmp_path):
        """With N user_principal_names, get_tasks should return N TaskWrappers."""
        users = ["alice@example.com", "bob@example.com", "carol@example.com"]
        config = make_audit_config(user_principal_names=users)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = AuditCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == len(users)
        for task, user in zip(tasks, users):
            assert isinstance(task, TaskWrapper)
            assert user in task.name

    # ------------------------------------------ #
    # get_tasks: empty list                      #
    # ------------------------------------------ #

    def test_get_tasks_empty_user_list_returns_single_task(self, tmp_path):
        """An empty user_principal_names list should fall back to a single task."""
        config = make_audit_config(user_principal_names=[])
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = AuditCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert tasks[0].name == "crawl_directory_audits"

    # ------------------------------------------ #
    # get_tasks: coroutine is set                #
    # ------------------------------------------ #

    def test_get_tasks_coroutine_is_set(self, tmp_path):
        """Every returned TaskWrapper must have a coroutine attribute."""
        config = make_audit_config(user_principal_names=["user@example.com"])
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = AuditCrawler(**kwargs)

        tasks = crawler.get_tasks()

        for task in tasks:
            assert hasattr(task, "coroutine")
            assert task.coroutine is not None
            task.coroutine.close()


class TestSignInCrawler(DateFieldTestMixin):
    """Tests for the SignInCrawler."""

    crawler_class = SignInCrawler
    config_factory = staticmethod(make_signin_config)
    expected_retention = RETENTION_SIGN_IN

    # ------------------------------------------ #
    # get_tasks: no user_principal_names         #
    # ------------------------------------------ #

    @pytest.mark.parametrize(
        "sign_in_type",
        [
            SignInType.INTERACTIVE_USER,
            SignInType.NON_INTERACTIVE_USER,
            SignInType.USER,
            SignInType.SERVICE_PRINCIPAL,
            SignInType.MANAGED_IDENTITY,
        ],
        ids=lambda t: t.value,
    )
    def test_get_tasks_no_users_returns_single_task(self, tmp_path, sign_in_type):
        """Without user_principal_names, get_tasks should return exactly one TaskWrapper."""
        config = make_signin_config(sign_in_type=sign_in_type)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)
        assert tasks[0].name == "crawl_signin"

    # ------------------------------------------ #
    # get_tasks: single user_principal_name      #
    # ------------------------------------------ #

    @pytest.mark.parametrize(
        "sign_in_type",
        [
            SignInType.INTERACTIVE_USER,
            SignInType.NON_INTERACTIVE_USER,
            SignInType.USER,
            SignInType.SERVICE_PRINCIPAL,
            SignInType.MANAGED_IDENTITY,
        ],
        ids=lambda t: t.value,
    )
    def test_get_tasks_single_user_returns_one_task(self, tmp_path, sign_in_type):
        """With one user_principal_name, get_tasks should return one TaskWrapper per user."""
        config = make_signin_config(
            sign_in_type=sign_in_type,
            user_principal_names=["user@example.com"],
        )
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)
        assert "user@example.com" in tasks[0].name

    # ------------------------------------------ #
    # get_tasks: multiple user_principal_names   #
    # ------------------------------------------ #

    @pytest.mark.parametrize(
        "sign_in_type",
        [
            SignInType.INTERACTIVE_USER,
            SignInType.NON_INTERACTIVE_USER,
            SignInType.USER,
            SignInType.SERVICE_PRINCIPAL,
            SignInType.MANAGED_IDENTITY,
        ],
        ids=lambda t: t.value,
    )
    def test_get_tasks_multiple_users_returns_task_per_user(self, tmp_path, sign_in_type):
        """With N user_principal_names, get_tasks should return N TaskWrappers."""
        users = ["jon@example.com", "doe@example.com", "jondoe@example.com"]
        config = make_signin_config(
            sign_in_type=sign_in_type,
            user_principal_names=users,
        )
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == len(users)
        for task, user in zip(tasks, users):
            assert isinstance(task, TaskWrapper)
            assert user in task.name

    # ------------------------------------------ #
    # get_tasks: service principal / managed id  #
    # ------------------------------------------ #

    @pytest.mark.parametrize(
        "sign_in_type",
        [SignInType.SERVICE_PRINCIPAL, SignInType.MANAGED_IDENTITY],
        ids=lambda t: t.value,
    )
    def test_get_tasks_service_principal_types_returns_task(self, tmp_path, sign_in_type):
        """Service principal and managed identity sign-in types should produce tasks."""
        config = make_signin_config(
            sign_in_type=sign_in_type,
            user_principal_names=["sp@example.com"],
        )
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)

    # ------------------------------------------ #
    # get_tasks: empty list                      #
    # ------------------------------------------ #

    def test_get_tasks_empty_user_list_returns_single_task(self, tmp_path):
        """An empty user_principal_names list should fall back to a single task."""
        config = make_signin_config(user_principal_names=[])
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert tasks[0].name == "crawl_signin"

    # ------------------------------------------ #
    # get_tasks: coroutine is set                #
    # ------------------------------------------ #

    def test_get_tasks_coroutine_is_set(self, tmp_path):
        """Every returned TaskWrapper must have a coroutine attribute."""
        config = make_signin_config(user_principal_names=["user@example.com"])
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        tasks = crawler.get_tasks()

        for task in tasks:
            assert hasattr(task, "coroutine")
            assert task.coroutine is not None
            # Clean up coroutine to avoid RuntimeWarning
            task.coroutine.close()

    # ------------------------------------------ #
    # _build_custom_filter: user sign-in types   #
    # ------------------------------------------ #

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "sign_in_type",
        [SignInType.INTERACTIVE_USER, SignInType.NON_INTERACTIVE_USER, SignInType.USER],
        ids=lambda t: t.value,
    )
    async def test_build_filter_user_type_with_user_id(self, tmp_path, sign_in_type):
        """User sign-in types with a resolved user ID should include both UPN and UserId."""
        config = make_signin_config(sign_in_type=sign_in_type)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        with patch.object(crawler, "_get_user_id", new_callable=AsyncMock, return_value="fake-user-id-123"):
            result = await crawler._build_custom_filter("user@example.com")

        assert "userPrincipalName eq 'user@example.com'" in result
        assert "UserId eq 'fake-user-id-123'" in result
        assert sign_in_type.odata_filter in result

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "sign_in_type",
        [SignInType.INTERACTIVE_USER, SignInType.NON_INTERACTIVE_USER, SignInType.USER],
        ids=lambda t: t.value,
    )
    async def test_build_filter_user_type_without_user_id(self, tmp_path, sign_in_type):
        """User sign-in types where _get_user_id returns None should filter by UPN only."""
        config = make_signin_config(sign_in_type=sign_in_type)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        with patch.object(crawler, "_get_user_id", new_callable=AsyncMock, return_value=None):
            result = await crawler._build_custom_filter("user@example.com")

        assert "userPrincipalName eq 'user@example.com'" in result
        assert "UserId" not in result
        assert sign_in_type.odata_filter in result

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "sign_in_type",
        [SignInType.INTERACTIVE_USER, SignInType.NON_INTERACTIVE_USER, SignInType.USER],
        ids=lambda t: t.value,
    )
    async def test_build_filter_user_type_no_upn(self, tmp_path, sign_in_type):
        """User sign-in types without a UPN should filter by sign-in type only.

        Currently fails because _build_custom_filter does not handle
        user_principal_name=None in the user-type branch.
        """
        config = make_signin_config(sign_in_type=sign_in_type)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        result = await crawler._build_custom_filter(None)

        assert "userPrincipalName" not in result
        assert sign_in_type.odata_filter in result

    # ------------------------------------------ #
    # _build_custom_filter: service principals   #
    # ------------------------------------------ #

    @pytest.mark.asyncio
    async def test_build_filter_service_principal(self, tmp_path):
        """Service principal type should filter by ServicePrincipalId."""
        config = make_signin_config(sign_in_type=SignInType.SERVICE_PRINCIPAL)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        result = await crawler._build_custom_filter("sp@example.com")

        assert "ServicePrincipalId eq 'sp@example.com'" in result
        assert SignInType.SERVICE_PRINCIPAL.odata_filter in result

    @pytest.mark.asyncio
    async def test_build_filter_managed_identity(self, tmp_path):
        """Managed identity type should filter by ServicePrincipalId."""
        config = make_signin_config(sign_in_type=SignInType.MANAGED_IDENTITY)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        result = await crawler._build_custom_filter("mi@example.com")

        assert "ServicePrincipalId eq 'mi@example.com'" in result
        assert SignInType.MANAGED_IDENTITY.odata_filter in result

    # ------------------------------------------ #
    # _build_custom_filter: base filter          #
    # ------------------------------------------ #

    @pytest.mark.asyncio
    async def test_build_filter_always_contains_date_placeholders(self, tmp_path):
        """Every filter string should contain the date range placeholders."""
        config = make_signin_config(sign_in_type=SignInType.USER)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        with patch.object(crawler, "_get_user_id", new_callable=AsyncMock, return_value=None):
            result = await crawler._build_custom_filter("user@example.com")

        assert "{filter_timstamp_name}" in result
        assert "{date_start}" in result
        assert "{date_end}" in result

    # ------------------------------------------ #
    # crawl_signin: end-to-end with mocked API   #
    # ------------------------------------------ #

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "sign_in_type",
        [
            SignInType.INTERACTIVE_USER,
            SignInType.NON_INTERACTIVE_USER,
            SignInType.USER,
            SignInType.SERVICE_PRINCIPAL,
            SignInType.MANAGED_IDENTITY,
        ],
        ids=lambda t: t.value,
    )
    async def test_crawl_signin_calls_simple_graph_query(self, tmp_path, sign_in_type):
        """crawl_signin should call simple_graph_query with the correct parameters."""
        config = make_signin_config(sign_in_type=sign_in_type)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        with (
            patch.object(crawler, "_get_user_id", new_callable=AsyncMock, return_value=None),
            patch.object(crawler, "simple_graph_query", new_callable=AsyncMock) as mock_query,
        ):
            await crawler.crawl_signin("user@example.com")

        mock_query.assert_called_once()
        call_kwargs = mock_query.call_args[1]
        assert call_kwargs["filter_timstamp_name"] == "createdDateTime"
        assert call_kwargs["request_func"] == "audit_logs.sign_ins"
        assert call_kwargs["split_days"] is True
        assert call_kwargs["number_interval_days"] == config.number_interval_days

    @pytest.mark.asyncio
    async def test_crawl_signin_without_user(self, tmp_path):
        """crawl_signin without a user should still call simple_graph_query."""
        config = make_signin_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = SignInCrawler(**kwargs)

        with patch.object(crawler, "simple_graph_query", new_callable=AsyncMock) as mock_query:
            await crawler.crawl_signin()

        mock_query.assert_called_once()


class TestUalCrawler(DateFieldTestMixin):
    """Tests for the UalCrawler."""

    crawler_class = UalCrawler
    config_factory = staticmethod(make_ual_config)
    expected_retention = RETENTION_UAL

    def test_get_tasks_returns_single_task(self, tmp_path):
        """UAL crawler always returns exactly one task."""
        config = make_ual_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = UalCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)
        assert tasks[0].name == "crawl_ual"

    async def test_crawl_ual_calls_ensure_graph_client(self, tmp_path):
        """crawl_ual should call ensure_graph_client; when it returns None, exit early."""
        config = make_ual_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = UalCrawler(**kwargs)

        with patch.object(crawler, "ensure_graph_client", new_callable=AsyncMock, return_value=None):
            crawler.graph_client = None
            await crawler.crawl_ual()


class TestMessageTracesCrawler(DateFieldTestMixin):
    """Tests for the MessageTracesCrawler."""

    crawler_class = MessageTracesCrawler
    config_factory = staticmethod(make_message_traces_config)
    expected_retention = RETENTION_MESSAGE_TRACES

    def test_get_tasks_returns_single_task(self, tmp_path):
        """Message traces crawler always returns exactly one task."""
        config = make_message_traces_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessageTracesCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)
        assert tasks[0].name == "crawl_message_traces"

    async def test_crawl_message_traces_calls_simple_graph_query(self, tmp_path):
        """crawl_message_traces should call simple_graph_query with correct params."""
        config = make_message_traces_config(sender_address="s@example.com")
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessageTracesCrawler(**kwargs)

        with patch.object(crawler, "simple_graph_query", new_callable=AsyncMock) as mock_query:
            await crawler.crawl_message_traces()

        mock_query.assert_called_once()
        call_kwargs = mock_query.call_args[1]
        assert call_kwargs["filter_timstamp_name"] == "receivedDateTime"
        assert call_kwargs["request_func"] == "admin.exchange.message_traces"


class TestMessageTracesPWSHCrawler(DateFieldTestMixin):
    """Tests for the MessageTracesPWSHCrawler."""

    crawler_class = MessageTracesPWSHCrawler
    config_factory = staticmethod(make_message_traces_pwsh_config)
    expected_retention = RETENTION_MESSAGE_TRACES

    def test_get_tasks_sender_only(self, tmp_path):
        """With only sender_addresses, one task per sender per date interval."""
        from datetime import timedelta

        now = datetime.now()
        config = make_message_traces_pwsh_config(
            sender_addresses=["s@example.com"],
            recipient_addresses=[],
            date_start=now - timedelta(days=5),
            date_end=now,
            number_interval_days=10,
        )
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessageTracesPWSHCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) >= 1
        assert all(isinstance(t, TaskWrapper) for t in tasks)
        assert all("from_s@example.com" in t.name for t in tasks)

    def test_get_tasks_recipient_only(self, tmp_path):
        """With only recipient_addresses, one task per recipient per date interval."""
        from datetime import timedelta

        now = datetime.now()
        config = make_message_traces_pwsh_config(
            sender_addresses=[],
            recipient_addresses=["r@example.com"],
            date_start=now - timedelta(days=5),
            date_end=now,
            number_interval_days=10,
        )
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessageTracesPWSHCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) >= 1
        assert all(isinstance(t, TaskWrapper) for t in tasks)
        assert all("to_r@example.com" in t.name for t in tasks)

    def test_get_tasks_sender_and_recipient(self, tmp_path):
        """With both senders and recipients, tasks are the cartesian product."""
        from datetime import timedelta

        now = datetime.now()
        config = make_message_traces_pwsh_config(
            sender_addresses=["s1@example.com", "s2@example.com"],
            recipient_addresses=["r1@example.com"],
            date_start=now - timedelta(days=5),
            date_end=now,
            number_interval_days=10,
        )
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessageTracesPWSHCrawler(**kwargs)

        tasks = crawler.get_tasks()

        # 2 senders × 1 recipient × 1 date interval = 2 tasks
        assert len(tasks) == 2
        assert all(isinstance(t, TaskWrapper) for t in tasks)

    def test_get_tasks_no_sender_no_recipient_returns_empty(self, tmp_path):
        """With neither senders nor recipients, no tasks should be generated."""
        from datetime import timedelta

        now = datetime.now()
        config = make_message_traces_pwsh_config(
            sender_addresses=[],
            recipient_addresses=[],
            date_start=now - timedelta(days=5),
            date_end=now,
            number_interval_days=10,
        )
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessageTracesPWSHCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert tasks == []


class TestMessagesCrawler(DateFieldTestMixin):
    """Tests for the MessagesCrawler."""

    crawler_class = MessagesCrawler
    config_factory = staticmethod(make_messages_config)
    expected_retention = RETENTION_MESSAGES

    def test_get_tasks_returns_task_per_user(self, tmp_path):
        """One task per user_principal_name."""
        users = ["a@example.com", "b@example.com"]
        config = make_messages_config(user_principal_names=users)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessagesCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == len(users)
        assert all(isinstance(t, TaskWrapper) for t in tasks)

    async def test_crawl_messages_calls_graph_api(self, tmp_path):
        """crawl_messages should ensure a graph client and call the pagination helper."""
        from unittest.mock import MagicMock

        config = make_messages_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessagesCrawler(**kwargs)

        # Build a mock graph_client with the correct sync attribute chain
        mock_messages = MagicMock()
        mock_by_user = MagicMock()
        mock_by_user.messages = mock_messages
        mock_users = MagicMock()
        mock_users.by_user_id.return_value = mock_by_user
        mock_graph = MagicMock()
        mock_graph.users = mock_users

        with (
            patch.object(crawler, "ensure_graph_client", new_callable=AsyncMock),
            patch.object(crawler, "make_graph_request_with_retry_and_pagination", new_callable=AsyncMock) as mock_pag,
        ):
            crawler.graph_client = mock_graph
            await crawler.crawl_messages("test@example.com")

        mock_pag.assert_called_once()


class TestMessageCrawler:
    """Tests for the single MessageCrawler."""

    def test_get_tasks_returns_single_task(self, tmp_path):
        """Message crawler always returns exactly one task."""
        config = make_message_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessageCrawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)
        assert tasks[0].name == "crawl_message"

    def test_retention_is_default(self, tmp_path):
        """MessageCrawler does not override RETENTION, so it should use RETENTION_DEFAULT."""
        config = make_message_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = MessageCrawler(**kwargs)

        assert crawler.RETENTION == RETENTION_DEFAULT


class TestM365Crawler:
    """Tests for the M365Crawler (boolean feature flags)."""

    def test_get_tasks_no_flags_enabled(self, tmp_path):
        """With all flags False, get_tasks should return an empty list."""
        config = make_m365_config()
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = M365Crawler(**kwargs)

        tasks = crawler.get_tasks()

        assert tasks == []

    def test_get_tasks_one_flag_enabled(self, tmp_path):
        """Enabling a single flag should produce exactly one task."""
        config = make_m365_config(users=True)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = M365Crawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 1
        assert isinstance(tasks[0], TaskWrapper)

    def test_get_tasks_multiple_flags_enabled(self, tmp_path):
        """Enabling N flags should produce N tasks."""
        config = make_m365_config(users=True, groups=True, devices=True)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        crawler = M365Crawler(**kwargs)

        tasks = crawler.get_tasks()

        assert len(tasks) == 3
        assert all(isinstance(t, TaskWrapper) for t in tasks)
