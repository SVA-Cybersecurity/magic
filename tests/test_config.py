#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for magic.helpers.config module.
"""

import pytest
import yaml
from datetime import datetime, timezone
from pydantic import ValidationError
from magic.helpers.config import (
    BaseAuditConfig,
    M365UALConfig,
    M365SigninConfig,
    M365AuditConfig,
    M365MessageTracesConfig,
    M365MessageTracesPWSHConfig,
    M365MessagesConfig,
    M365MessageConfig,
    M365Config,
    CrawlConfig,
    EnrichConfig,
    Settings,
    Defaults,
    SignInType,
    set_defaults,
    parse_config,
)


@pytest.fixture
def minimal_settings_dict():
    """Minimal valid settings block as a dict."""
    return {
        "permission_preflight_check": False,
        "auth": {
            "client_secret": "secret",
            "client_id": "id",
            "tenant_id": "tid",
        },
        "defaults": {
            "user_principal_names": None,
            "date_start": None,
            "date_end": None,
        },
    }


@pytest.fixture
def minimal_enrich_dict():
    """Minimal valid enrich block as a dict."""
    return {
        "timesketch": {"enabled": False, "output_filename": "ts.jsonl"},
        "ipapi": {"enabled": False, "output_filename": "ipapi.jsonl"},
        "hash": {
            "enabled": False,
            "output_filename": "hash.jsonl",
            "output_filename_csv": "hash.csv",
        },
    }


class TestBaseAuditConfig:
    """Tests for the BaseAuditConfig date-range validator."""

    def test_valid_date_range(self):
        """date_start before date_end should be accepted."""
        cfg = BaseAuditConfig(
            date_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            date_end=datetime(2024, 1, 31, tzinfo=timezone.utc),
        )
        assert cfg.date_start < cfg.date_end

    def test_date_start_after_date_end_raises(self):
        """date_start after date_end must raise a ValidationError."""
        with pytest.raises(ValidationError, match="date_start"):
            BaseAuditConfig(
                date_start=datetime(2024, 2, 1, tzinfo=timezone.utc),
                date_end=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )

    def test_both_dates_none(self):
        """Omitting both dates should be valid (defaults to None)."""
        cfg = BaseAuditConfig()
        assert cfg.date_start is None
        assert cfg.date_end is None

    def test_only_date_end_set(self):
        """Setting only date_end without date_start should be valid."""
        cfg = BaseAuditConfig(
            date_end=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        assert cfg.date_start is None
        assert cfg.date_end is not None


class TestM365UALConfig:
    """Tests for the Unified Audit Log crawl config."""

    def test_defaults(self):
        """Creating with no arguments should apply sane defaults."""
        cfg = M365UALConfig()
        assert cfg.type == "m365_ual"
        assert cfg.search_name_prefix == "DataCrawler"
        assert cfg.number_interval_days == 7
        assert cfg.record_types == []
        assert cfg.operations == []

    def test_none_to_list_coercion(self):
        """None values for list fields should be coerced to empty lists."""
        cfg = M365UALConfig(record_types=None, operations=None, ip_addresses=None)
        assert cfg.record_types == []
        assert cfg.operations == []
        assert cfg.ip_addresses == []

    def test_custom_values(self):
        """Explicit values should override defaults."""
        cfg = M365UALConfig(
            search_name_prefix="Custom",
            keyword="test",
            number_interval_days=3,
            record_types=["AzureActiveDirectory"],
        )
        assert cfg.search_name_prefix == "Custom"
        assert cfg.keyword == "test"
        assert cfg.number_interval_days == 3
        assert cfg.record_types == ["AzureActiveDirectory"]


class TestM365SigninConfig:
    """Tests for the Sign-In crawl config and SignInType enum."""

    def test_defaults(self):
        """Default sign_in_type should be 'user'."""
        cfg = M365SigninConfig()
        assert cfg.type == "m365_signin"
        assert cfg.sign_in_type == SignInType.USER

    @pytest.mark.parametrize(
        "value, expected_enum",
        [
            ("interactiveUser", SignInType.INTERACTIVE_USER),
            ("nonInteractiveUser", SignInType.NON_INTERACTIVE_USER),
            ("user", SignInType.USER),
            ("servicePrincipal", SignInType.SERVICE_PRINCIPAL),
            ("managedIdentity", SignInType.MANAGED_IDENTITY),
        ],
    )
    def test_sign_in_type_values(self, value, expected_enum):
        """Each SignInType string value should map to the correct enum member."""
        cfg = M365SigninConfig(sign_in_type=value)
        assert cfg.sign_in_type == expected_enum

    def test_invalid_sign_in_type_raises(self):
        """An unknown sign_in_type string must raise a ValidationError."""
        with pytest.raises(ValidationError):
            M365SigninConfig(sign_in_type="unknown")

    def test_odata_filter_property(self):
        """The odata_filter property should return a valid OData fragment."""
        assert "interactiveUser" in SignInType.USER.odata_filter


class TestM365AuditConfig:
    """Tests for the Audit crawl config."""

    def test_defaults(self):
        """Default interval should be 7 days."""
        cfg = M365AuditConfig()
        assert cfg.type == "m365_audit"
        assert cfg.number_interval_days == 7

    def test_with_user_principal_names(self):
        """Valid email addresses should be accepted."""
        cfg = M365AuditConfig(user_principal_names=["user@example.com"])
        assert cfg.user_principal_names == ["user@example.com"]

    def test_invalid_email_raises(self):
        """An invalid email should raise a ValidationError."""
        with pytest.raises(ValidationError):
            M365AuditConfig(user_principal_names=["not-an-email"])


class TestM365MessageTracesConfig:
    """Tests for the Message Traces crawl config."""

    def test_defaults(self):
        """Default values should be None for optional fields."""
        cfg = M365MessageTracesConfig()
        assert cfg.type == "m365_message_traces"
        assert cfg.from_ip is None
        assert cfg.subject is None
        assert cfg.number_interval_days == 7

    @pytest.mark.parametrize("filter_type", ["Contains", "EndsWith", "StartsWith"])
    def test_valid_subject_filter_types(self, filter_type):
        """Valid subject_filter_type values should pass validation."""
        cfg = M365MessageTracesConfig(subject="test", subject_filter_type=filter_type)
        assert cfg.subject_filter_type == filter_type

    def test_invalid_subject_filter_type_raises(self):
        """An invalid subject_filter_type with a subject set must raise."""
        with pytest.raises(ValidationError, match="subject_filter_type"):
            M365MessageTracesConfig(subject="test", subject_filter_type="InvalidFilter")


class TestM365MessageTracesPWSHConfig:
    """Tests for the PowerShell Message Traces crawl config."""

    def test_sender_or_recipient_required(self):
        """Omitting both sender_addresses and recipient_addresses must raise."""
        with pytest.raises(ValidationError, match="sender_addresses|recipient_addresses"):
            M365MessageTracesPWSHConfig(sender_addresses=None, recipient_addresses=None)

    def test_valid_with_sender(self):
        """Providing sender_addresses alone should be valid."""
        cfg = M365MessageTracesPWSHConfig(sender_addresses=["a@example.com"])
        assert cfg.sender_addresses == ["a@example.com"]

    def test_valid_with_recipient(self):
        """Providing recipient_addresses alone should be valid."""
        cfg = M365MessageTracesPWSHConfig(recipient_addresses=["b@example.com"])
        assert cfg.recipient_addresses == ["b@example.com"]

    def test_number_interval_days_default(self):
        """Default interval should be 10 days."""
        cfg = M365MessageTracesPWSHConfig(sender_addresses=["a@example.com"])
        assert cfg.number_interval_days == 10

    def test_number_interval_days_too_high_raises(self):
        """Interval > 10 must raise a ValidationError."""
        with pytest.raises(ValidationError, match="number_interval_days"):
            M365MessageTracesPWSHConfig(
                sender_addresses=["a@example.com"],
                number_interval_days=11,
            )

    def test_number_interval_days_too_low_raises(self):
        """Interval < 1 must raise a ValidationError."""
        with pytest.raises(ValidationError, match="number_interval_days"):
            M365MessageTracesPWSHConfig(
                sender_addresses=["a@example.com"],
                number_interval_days=0,
            )

    @pytest.mark.parametrize("filter_type", ["Contains", "EndsWith", "StartsWith"])
    def test_valid_subject_filter_types(self, filter_type):
        """Valid subject_filter_type values should pass validation."""
        cfg = M365MessageTracesPWSHConfig(
            sender_addresses=["a@example.com"],
            subject="test",
            subject_filter_type=filter_type,
        )
        assert cfg.subject_filter_type == filter_type


class TestM365MessagesConfig:
    """Tests for the Messages crawl config."""

    def test_missing_user_principal_names_raises(self):
        """user_principal_names is required and must raise when absent."""
        with pytest.raises(ValidationError, match="user_principal_names"):
            M365MessagesConfig()

    def test_valid_user_principal_names(self):
        """A list with valid emails should be accepted."""
        cfg = M365MessagesConfig(user_principal_names=["u@example.com"])
        assert cfg.user_principal_names == ["u@example.com"]


class TestM365MessageConfig:
    """Tests for the single Message crawl config."""

    def test_missing_user_principal_name_raises(self):
        """user_principal_name is required."""
        with pytest.raises(ValidationError, match="user_principal_name"):
            M365MessageConfig(message_id="abc")

    def test_missing_message_ids_raises(self):
        """At least one of message_id or internet_message_id is required."""
        with pytest.raises(ValidationError, match="message_id|internet_message_id"):
            M365MessageConfig(user_principal_name="u@example.com")

    def test_valid_with_message_id(self):
        """Providing message_id should be valid."""
        cfg = M365MessageConfig(user_principal_name="u@example.com", message_id="id-123")
        assert cfg.message_id == "id-123"

    def test_valid_with_internet_message_id(self):
        """Providing internet_message_id should be valid."""
        cfg = M365MessageConfig(
            user_principal_name="u@example.com",
            internet_message_id="<msg@example.com>",
        )
        assert cfg.internet_message_id == "<msg@example.com>"


class TestM365Config:
    """Tests for the general M365 crawl config (boolean feature flags)."""

    @pytest.fixture
    def all_false_config(self):
        """M365Config with every flag set to False."""
        fields = {f: False for f in M365Config.model_fields if f != "type"}
        return M365Config(**fields)

    def test_type_literal(self, all_false_config):
        """Type should always be 'm365'."""
        assert all_false_config.type == "m365"

    def test_iteration(self, all_false_config):
        """Iterating should yield field names (excluding 'type')."""
        keys = list(all_false_config)
        assert "type" not in keys
        assert "users" in keys

    def test_contains(self, all_false_config):
        """__contains__ should work for method fields."""
        assert "users" in all_false_config
        assert "type" not in all_false_config

    def test_items(self, all_false_config):
        """items() should return (key, value) pairs without 'type'."""
        items = dict(all_false_config.items())
        assert "type" not in items
        assert all(v is False for v in items.values())


class TestCrawlConfig:
    """Tests for the CrawlConfig RootModel with discriminated union."""

    def test_parse_mixed_crawl_list(self):
        """A list with different crawl types should be parsed correctly."""
        data = [
            {"type": "m365_ual"},
            {"type": "m365_signin"},
            {"type": "m365_audit"},
        ]
        config = CrawlConfig.model_validate(data)
        assert len(config.root) == 3
        assert isinstance(config.root[0], M365UALConfig)
        assert isinstance(config.root[1], M365SigninConfig)
        assert isinstance(config.root[2], M365AuditConfig)

    def test_unknown_type_raises(self):
        """An unknown type value should raise a ValidationError."""
        with pytest.raises(ValidationError):
            CrawlConfig.model_validate([{"type": "unknown_crawler"}])

    def test_empty_list(self):
        """An empty crawl list should be valid."""
        config = CrawlConfig.model_validate([])
        assert config.root == []


class TestEnrichConfig:
    """Tests for the EnrichConfig and its nested models."""

    def test_valid_enrich_config(self, minimal_enrich_dict):
        """A minimal enrich dict should parse without errors."""
        cfg = EnrichConfig(**minimal_enrich_dict)
        assert cfg.timesketch.enabled is False
        assert cfg.hash.output_filename == "hash.jsonl"

    def test_s3_upload_optional(self, minimal_enrich_dict):
        """s3_upload should default to None when omitted."""
        cfg = EnrichConfig(**minimal_enrich_dict)
        assert cfg.s3_upload is None

    def test_s3_upload_provided(self, minimal_enrich_dict):
        """s3_upload should be parsed when present."""
        minimal_enrich_dict["s3_upload"] = {
            "enabled": True,
            "bucket_path": "my-bucket/",
        }
        cfg = EnrichConfig(**minimal_enrich_dict)
        assert cfg.s3_upload.enabled is True
        assert cfg.s3_upload.bucket_path == "my-bucket/"

    def test_missing_required_field_raises(self):
        """Omitting a required sub-model should raise."""
        with pytest.raises(ValidationError):
            EnrichConfig(
                timesketch={"enabled": True, "output_filename": "ts.jsonl"},
                ipapi={"enabled": False, "output_filename": "ip.jsonl"},
                # hash is missing
            )


class TestSettings:
    """Tests for the Settings model."""

    def test_valid_settings(self, minimal_settings_dict):
        """A minimal settings dict should parse correctly."""
        s = Settings(**minimal_settings_dict)
        assert s.permission_preflight_check is False
        assert s.auth.client_id == "id"

    def test_optional_s3_defaults_none(self, minimal_settings_dict):
        """S3 settings should default to None when omitted."""
        s = Settings(**minimal_settings_dict)
        assert s.s3 is None

    def test_optional_ipapi_defaults_none(self, minimal_settings_dict):
        """IpAPI settings should default to None when omitted."""
        s = Settings(**minimal_settings_dict)
        assert s.ipapi is None

    def test_missing_auth_raises(self):
        """Omitting auth should raise a ValidationError."""
        with pytest.raises(ValidationError):
            Settings(
                permission_preflight_check=False,
                defaults={"user_principal_names": None, "date_start": None, "date_end": None},
            )


class TestSetDefaults:
    """Tests for the set_defaults helper function."""

    def test_applies_default_dates(self, mock_logger):
        """Default date values should be applied to actions missing them."""
        defaults = Defaults(
            date_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            date_end=datetime(2024, 1, 31, tzinfo=timezone.utc),
            user_principal_names=None,
        )
        actions = [{"type": "m365_ual", "date_start": None, "date_end": None}]

        result = set_defaults(actions, defaults, mock_logger)

        assert result[0]["date_start"] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert result[0]["date_end"] == datetime(2024, 1, 31, tzinfo=timezone.utc)

    def test_does_not_overwrite_existing_values(self, mock_logger):
        """Values already set on the action should not be overwritten."""
        defaults = Defaults(
            date_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            date_end=datetime(2024, 1, 31, tzinfo=timezone.utc),
            user_principal_names=None,
        )
        existing_start = datetime(2024, 3, 1, tzinfo=timezone.utc)
        actions = [{"type": "m365_ual", "date_start": existing_start, "date_end": None}]

        result = set_defaults(actions, defaults, mock_logger)

        assert result[0]["date_start"] == existing_start

    def test_ignores_keys_not_in_action(self, mock_logger):
        """Keys in defaults that don't exist on the action should be skipped."""
        defaults = Defaults(
            date_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            date_end=None,
            user_principal_names=["u@example.com"],
        )
        actions = [{"type": "m365_message", "date_start": None}]

        result = set_defaults(actions, defaults, mock_logger)
        # user_principal_names is not a key on this action dict, should not appear
        assert "user_principal_names" not in result[0]


class TestParseConfig:
    """End-to-end tests for parse_config using temporary YAML files."""

    def _write_yaml(self, tmp_path, data):
        """Helper to write a dict as a YAML file and return the path."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(data, default_flow_style=False))
        return str(config_file)

    def test_parse_minimal_config(self, tmp_path, mock_logger, minimal_settings_dict, minimal_enrich_dict):
        """A minimal but valid config file should parse into the three return values."""
        data = {
            "settings": minimal_settings_dict,
            "crawl": [{"type": "m365_ual"}],
            "enrich": minimal_enrich_dict,
        }
        path = self._write_yaml(tmp_path, data)

        settings, crawl_config, enrich_config = parse_config(path, mock_logger)

        assert isinstance(settings, Settings)
        assert isinstance(crawl_config, CrawlConfig)
        assert isinstance(enrich_config, EnrichConfig)
        assert len(crawl_config.root) == 1
        assert isinstance(crawl_config.root[0], M365UALConfig)

    def test_parse_config_multiple_crawls(self, tmp_path, mock_logger, minimal_settings_dict, minimal_enrich_dict):
        """Multiple crawl entries of different types should all be parsed."""
        m365_fields = {f: False for f in M365Config.model_fields if f != "type"}
        m365_fields["type"] = "m365"

        data = {
            "settings": minimal_settings_dict,
            "crawl": [
                {"type": "m365_ual"},
                {"type": "m365_signin"},
                m365_fields,
            ],
            "enrich": minimal_enrich_dict,
        }
        path = self._write_yaml(tmp_path, data)

        settings, crawl_config, enrich_config = parse_config(path, mock_logger)

        assert len(crawl_config.root) == 3

    def test_parse_config_applies_defaults(self, tmp_path, mock_logger, minimal_enrich_dict):
        """Defaults from settings should be propagated to crawl items."""
        settings_dict = {
            "permission_preflight_check": False,
            "auth": {"client_secret": "s", "client_id": "c", "tenant_id": "t"},
            "defaults": {
                "date_start": "2024-01-01T00:00:00+00:00",
                "date_end": "2024-01-31T00:00:00+00:00",
                "user_principal_names": None,
            },
        }
        data = {
            "settings": settings_dict,
            "crawl": [{"type": "m365_ual", "date_start": None, "date_end": None}],
            "enrich": minimal_enrich_dict,
        }
        path = self._write_yaml(tmp_path, data)

        settings, crawl_config, _ = parse_config(path, mock_logger)

        assert crawl_config.root[0].date_start is not None

    def test_parse_config_missing_file_raises(self, mock_logger):
        """A non-existent config file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            parse_config("/nonexistent/config.yaml", mock_logger)

    def test_parse_config_invalid_crawl_type_exits(self, tmp_path, mock_logger, minimal_settings_dict, minimal_enrich_dict):
        """An unknown crawl type in the YAML should cause a SystemExit (parse_config catches ValidationError)."""
        data = {
            "settings": minimal_settings_dict,
            "crawl": [{"type": "totally_invalid"}],
            "enrich": minimal_enrich_dict,
        }
        path = self._write_yaml(tmp_path, data)

        with pytest.raises(SystemExit):
            parse_config(path, mock_logger)
