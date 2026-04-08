#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Reusable test mixins for crawler test classes.
"""

import datetime
from unittest.mock import patch
from tests.factories import make_crawler_kwargs


class DateFieldTestMixin:
    """Tests for ``_read_date_fields`` behaviour.

    Any test class that mixes this in must define:

    ``crawler_class``
        The crawler class under test (e.g. ``AuditCrawler``).

    ``config_factory``
        A callable (factory function) that returns a valid config
        instance for this crawler, accepting ``**overrides``.

    ``expected_retention``
        The expected RETENTION value in days (e.g. ``30`` for Audit).
    """

    crawler_class = None
    config_factory = None
    expected_retention = None

    def _make_crawler(self, tmp_path, **config_overrides):
        """Helper to instantiate the crawler with a config built from the factory."""
        config = self.config_factory(**config_overrides)
        kwargs = make_crawler_kwargs(tmp_path, config=config)
        return self.crawler_class(**kwargs)

    def test_date_fields_from_config(self, tmp_path):
        """Explicit date_start and date_end from config should be used as-is."""
        date_start = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        date_end = datetime.datetime(2024, 1, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)

        crawler = self._make_crawler(tmp_path, date_start=date_start, date_end=date_end)
        result_start, result_end = crawler._read_date_fields()

        assert result_start == date_start
        assert result_end == date_end

    def test_date_fields_fallback_to_retention(self, tmp_path):
        """When no dates are set, date_start should fall back to now minus RETENTION days."""
        fixed_now = datetime.datetime(2024, 6, 15, 10, 30, 0)

        crawler = self._make_crawler(tmp_path)

        with patch("magic.interfaces.crawler.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = fixed_now
            mock_dt.timedelta = datetime.timedelta
            mock_dt.time = datetime.time
            mock_dt.date = datetime.date
            mock_dt.datetime.combine = datetime.datetime.combine
            result_start, result_end = crawler._read_date_fields()

        expected_start = (fixed_now - datetime.timedelta(days=self.expected_retention)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        assert result_start == expected_start

    def test_date_end_midnight_adjusted_to_end_of_day(self, tmp_path):
        """A date_end at midnight should be adjusted to 23:59:59.999999."""
        date_start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        date_end = datetime.datetime(2024, 1, 31, 0, 0, 0)

        crawler = self._make_crawler(tmp_path, date_start=date_start, date_end=date_end)
        _, result_end = crawler._read_date_fields()

        assert result_end.hour == 23
        assert result_end.minute == 59
        assert result_end.second == 59
        assert result_end.microsecond == 999999

    def test_date_end_non_midnight_not_adjusted(self, tmp_path):
        """A date_end with a non-midnight time should not be adjusted."""
        date_start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        date_end = datetime.datetime(2024, 1, 31, 14, 30, 0)

        crawler = self._make_crawler(tmp_path, date_start=date_start, date_end=date_end)
        _, result_end = crawler._read_date_fields()

        assert result_end == date_end

    def test_retention_value(self, tmp_path):
        """The crawler's RETENTION should match the expected value."""
        crawler = self._make_crawler(tmp_path)
        assert crawler.RETENTION == self.expected_retention
