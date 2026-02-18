#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for magic.helpers.utils module
"""

import pytest
import datetime
import json
import tempfile
import os
import asyncio
import logging
import logging.handlers
from io import StringIO
from uuid import UUID
from magic.helpers.utils import (
    daterange,
    is_midnight,
    end_of_day,
    next_midnight,
    date_string_in_file_identifier,
    snake_to_camel,
    convert_keys_to_camel_case,
    check_output_dir,
    custom_serializer,
    remove_odata_fields,
    write_json_to_file,
    log_task,
    semaphore_wrapper,
    TaskWrapper,
)


class TestDateRange:
    """Test cases for the daterange function"""

    def test_daterange_negative_interval_raises_error(self):
        """Test that negative interval raises ValueError"""
        start = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 5, tzinfo=datetime.timezone.utc)

        with pytest.raises(ValueError, match="number_interval_days must be non-negative"):
            list(daterange(start, end, -1))

    def test_daterange_start_after_end_returns_empty(self):
        """Test that start_date > end_date returns empty generator"""
        start = datetime.datetime(2024, 1, 10, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 5, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 1))
        assert result == []

    def test_daterange_zero_interval_returns_single_range(self):
        """Test that interval of 0 returns the full range once"""
        start = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 31, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 0))
        assert len(result) == 1
        assert result[0] == (start, end)

    def test_daterange_midnight_start_single_day(self):
        """Test single day range starting at midnight"""
        start = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 1, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 1))
        assert len(result) == 1
        assert result[0][0] == start
        assert result[0][1] == end

    def test_daterange_midnight_start_multiple_days(self):
        """Test multiple days with midnight start"""
        start = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 5, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 1))
        assert len(result) == 5

        # Check first range
        assert result[0][0] == datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result[0][1] == datetime.datetime(2024, 1, 1, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)

        # Check last range
        assert result[4][0] == datetime.datetime(2024, 1, 5, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result[4][1] == end

    def test_daterange_non_midnight_start(self):
        """Test range with non-midnight start time"""
        start = datetime.datetime(2024, 1, 1, 10, 30, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 3, 10, 30, 0, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 1))
        assert len(result) == 2

        # First interval should be 1 day from start
        assert result[0][0] == start
        assert result[0][1] == datetime.datetime(2024, 1, 2, 10, 30, 0, tzinfo=datetime.timezone.utc)

        # Second interval
        assert result[1][0] == datetime.datetime(2024, 1, 2, 10, 30, 0, tzinfo=datetime.timezone.utc)
        assert result[1][1] == end

    def test_daterange_interval_larger_than_range(self):
        """Test when interval is larger than the total range"""
        start = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 2, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 10))
        assert len(result) == 1
        assert result[0][0] == start
        assert result[0][1] == end

    def test_daterange_two_day_interval(self):
        """Test 2-day intervals"""
        start = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 6, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 2))
        assert len(result) == 3

        # First interval: Jan 1-2
        assert result[0][0] == datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result[0][1] == datetime.datetime(2024, 1, 2, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)

        # Second interval: Jan 3-4
        assert result[1][0] == datetime.datetime(2024, 1, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result[1][1] == datetime.datetime(2024, 1, 4, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)

        # Third interval: Jan 5-6
        assert result[2][0] == datetime.datetime(2024, 1, 5, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result[2][1] == end

    def test_daterange_same_start_and_end(self):
        """Test when start and end are the same"""
        start = datetime.datetime(2024, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 1))
        assert len(result) == 1
        assert result[0] == (start, end)

    def test_daterange_partial_final_interval(self):
        """Test that final interval is capped at end_date"""
        start = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2024, 1, 4, 12, 0, 0, tzinfo=datetime.timezone.utc)

        result = list(daterange(start, end, 2))

        # Last interval should end at end_date, not extend beyond
        assert result[-1][1] == end
        assert result[-1][1] <= end


class TestIsMidnight:
    """Test cases for is_midnight helper function"""

    def test_is_midnight_true_zero_time(self):
        """Test midnight at 00:00:00"""
        dt = datetime.datetime(2024, 1, 1, 0, 0, 0, 0)
        assert is_midnight(dt) is True

    def test_is_midnight_true_end_of_day(self):
        """Test end of day (23:59:59.999999)"""
        dt = datetime.datetime(2024, 1, 1, 23, 59, 59, 999999)
        assert is_midnight(dt) is True

    def test_is_midnight_false(self):
        """Test non-midnight time"""
        dt = datetime.datetime(2024, 1, 1, 12, 30, 0, 0)
        assert is_midnight(dt) is False

    def test_is_midnight_false_almost_midnight(self):
        """Test time just before end of day"""
        dt = datetime.datetime(2024, 1, 1, 23, 59, 58, 999999)
        assert is_midnight(dt) is False


class TestEndOfDay:
    """Test cases for end_of_day function"""

    def test_end_of_day_preserves_date(self):
        """Test that date is preserved"""
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45)
        result = end_of_day(dt)
        assert result.date() == dt.date()

    def test_end_of_day_sets_time(self):
        """Test that time is set to 23:59:59.999999"""
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45)
        result = end_of_day(dt)
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59
        assert result.microsecond == 999999

    def test_end_of_day_preserves_timezone(self):
        """Test that timezone is preserved"""
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45, tzinfo=datetime.timezone.utc)
        result = end_of_day(dt)
        assert result.tzinfo == datetime.timezone.utc


class TestNextMidnight:
    """Test cases for next_midnight function"""

    def test_next_midnight_adds_day(self):
        """Test that next midnight is the next day"""
        dt = datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=datetime.timezone.utc)
        result = next_midnight(dt)
        expected = datetime.datetime(2024, 1, 16, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_next_midnight_month_boundary(self):
        """Test next midnight across month boundary"""
        dt = datetime.datetime(2024, 1, 31, 10, 30, 45, tzinfo=datetime.timezone.utc)
        result = next_midnight(dt)
        expected = datetime.datetime(2024, 2, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        assert result == expected

    def test_next_midnight_preserves_timezone(self):
        """Test that timezone is preserved"""
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45, tzinfo=datetime.timezone.utc)
        result = next_midnight(dt)
        assert result.tzinfo == datetime.timezone.utc


class TestDateStringInFileIdentifier:
    """Test cases for date_string_in_file_identifier function"""

    def test_same_day_at_midnight(self):
        """Test same day with midnight times"""
        start = datetime.datetime(2024, 1, 15, 0, 0, 0)
        end = datetime.datetime(2024, 1, 15, 23, 59, 59, 999999)

        start_str, end_str = date_string_in_file_identifier(start, end)
        assert start_str == "2024-01-15"
        assert end_str == ""

    def test_different_days_at_midnight(self):
        """Test different days with midnight times"""
        start = datetime.datetime(2024, 1, 15, 0, 0, 0)
        end = datetime.datetime(2024, 1, 20, 23, 59, 59, 999999)

        start_str, end_str = date_string_in_file_identifier(start, end)
        assert start_str == "2024-01-15"
        assert end_str == "2024-01-20"

    def test_non_midnight_times(self):
        """Test with specific times (not midnight)"""
        start = datetime.datetime(2024, 1, 15, 10, 30, 0)
        end = datetime.datetime(2024, 1, 15, 14, 45, 30)

        start_str, end_str = date_string_in_file_identifier(start, end)
        assert start_str == "2024-01-15T10:30:00"
        assert end_str == "2024-01-15T14:45:30"


class TestSnakeToCamel:
    """Test cases for snake_to_camel function"""

    def test_single_word(self):
        """Test single word"""
        assert snake_to_camel("hello") == "Hello"

    def test_two_words(self):
        """Test two words"""
        assert snake_to_camel("hello_world") == "HelloWorld"

    def test_multiple_words(self):
        """Test multiple words"""
        assert snake_to_camel("this_is_a_test") == "ThisIsATest"

    def test_already_camel(self):
        """Test already camelCase-like input"""
        assert snake_to_camel("alreadyCamel") == "Alreadycamel"


class TestConvertKeysToCamelCase:
    """Test cases for convert_keys_to_camel_case function"""

    def test_simple_dict(self):
        """Test simple dictionary conversion"""
        input_dict = {"first_name": "John", "last_name": "Doe"}
        expected = {"FirstName": "John", "LastName": "Doe"}
        assert convert_keys_to_camel_case(input_dict) == expected

    def test_nested_dict(self):
        """Test nested dictionary conversion"""
        input_dict = {"user_info": {"first_name": "John", "last_name": "Doe"}}
        expected = {"UserInfo": {"FirstName": "John", "LastName": "Doe"}}
        assert convert_keys_to_camel_case(input_dict) == expected

    def test_list_of_dicts(self):
        """Test list of dictionaries"""
        input_list = [{"first_name": "John"}, {"last_name": "Doe"}]
        expected = [{"FirstName": "John"}, {"LastName": "Doe"}]
        assert convert_keys_to_camel_case(input_list) == expected

    def test_mixed_types(self):
        """Test with mixed types"""
        input_dict = {"user_name": "john", "user_age": 30, "is_active": True}
        expected = {"UserName": "john", "UserAge": 30, "IsActive": True}
        assert convert_keys_to_camel_case(input_dict) == expected

    def test_empty_dict(self):
        """Test empty dictionary"""
        assert convert_keys_to_camel_case({}) == {}

    def test_primitive_value(self):
        """Test primitive value returns unchanged"""
        assert convert_keys_to_camel_case("test") == "test"
        assert convert_keys_to_camel_case(123) == 123


class TestCheckOutputDir:
    """Test cases for check_output_dir function"""

    def test_creates_nonexistent_directory(self):
        """Test that function creates directory if it doesn't exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_dir = os.path.join(tmpdir, "test_output")
            assert not os.path.exists(test_dir)

            check_output_dir(test_dir, None)

            assert os.path.exists(test_dir)
            assert os.path.isdir(test_dir)

    def test_accepts_existing_directory(self):
        """Test that function accepts existing directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Should not raise an error
            check_output_dir(tmpdir, None)
            assert os.path.isdir(tmpdir)

    def test_raises_error_if_path_is_file(self):
        """Test that function exits if path exists but is a file"""
        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            tmpfile_path = tmpfile.name

        try:
            with pytest.raises(SystemExit):
                check_output_dir(tmpfile_path, None)
        finally:
            os.unlink(tmpfile_path)

    def test_creates_nested_directories(self):
        """Test that function creates nested directories"""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_dir = os.path.join(tmpdir, "level1", "level2", "level3")

            check_output_dir(nested_dir, None)

            assert os.path.exists(nested_dir)
            assert os.path.isdir(nested_dir)


class TestCustomSerializer:
    """Test cases for custom_serializer function"""

    def test_serializes_datetime(self):
        """Test datetime serialization"""
        dt = datetime.datetime(2024, 6, 15, 10, 30, 45)
        result = custom_serializer(dt)
        assert result == "2024-06-15T10:30:45"

    def test_serializes_time(self):
        """Test time serialization"""
        t = datetime.time(14, 30, 0)
        result = custom_serializer(t)
        assert result == "H:i:s"  # Based on the strftime format in the code

    def test_serializes_uuid(self):
        """Test UUID serialization"""
        test_uuid = UUID("12345678-1234-5678-1234-567812345678")
        result = custom_serializer(test_uuid)
        assert result == "12345678-1234-5678-1234-567812345678"

    def test_serializes_int_to_string(self):
        """Test integer serialization to string"""
        result = custom_serializer(42)
        assert result == "42"
        assert isinstance(result, str)

    def test_serializes_string_unchanged(self):
        """Test string passes through unchanged"""
        result = custom_serializer("test string")
        assert result == "test string"

    def test_serializes_bytes(self):
        """Test bytes serialization"""
        result = custom_serializer(b"hello")
        assert result == "hello"

    def test_serializes_object_with_dict(self):
        """Test object with __dict__ serialization"""

        class TestObj:
            def __init__(self):
                self.first_name = "John"
                self.last_name = "Doe"
                self.backing_store = "ignore"
                self.empty_field = None
                self.is_active = "true"
                self.is_deleted = "false"

        obj = TestObj()
        result = custom_serializer(obj)

        assert result == {"FirstName": "John", "LastName": "Doe", "IsActive": True, "IsDeleted": False}

    def test_raises_type_error_for_unsupported_type(self):
        """Test that unsupported types raise TypeError"""
        with pytest.raises(TypeError, match="not serializable"):
            custom_serializer(set([1, 2, 3]))


class TestRemoveOdataFields:
    """Test cases for remove_odata_fields function"""

    def test_removes_odata_fields(self):
        """Test removal of @odata prefixed fields"""
        input_dict = {"name": "John", "@odata.context": "some context", "@odata.type": "#Microsoft.Graph.User", "age": 30}
        result = remove_odata_fields(input_dict)
        assert result == {"name": "John", "age": 30}

    def test_removes_odatatype_fields(self):
        """Test removal of OdataType fields"""
        input_dict = {"name": "John", "OdataType": "#Microsoft.Graph.User", "age": 30}
        result = remove_odata_fields(input_dict)
        assert result == {"name": "John", "age": 30}

    def test_preserves_normal_fields(self):
        """Test that normal fields are preserved"""
        input_dict = {"name": "John", "email": "john@example.com", "active": True}
        result = remove_odata_fields(input_dict)
        assert result == input_dict

    def test_empty_dict(self):
        """Test with empty dictionary"""
        result = remove_odata_fields({})
        assert result == {}


class TestWriteJsonToFile:
    """Test cases for write_json_to_file function"""

    def test_writes_simple_json(self):
        """Test writing simple JSON object"""
        data = {"name": "John", "age": 30}
        output = StringIO()

        write_json_to_file(data, output)

        result = output.getvalue()
        assert result.endswith('\n')
        loaded = json.loads(result.strip())
        assert loaded == data

    def test_removes_odata_fields(self):
        """Test that @odata fields are removed"""
        data = {"name": "John", "@odata.context": "remove me", "age": 30}
        output = StringIO()

        write_json_to_file(data, output)

        result = json.loads(output.getvalue().strip())
        assert "@odata.context" not in result
        assert result == {"name": "John", "age": 30}

    def test_handles_additional_data(self):
        """Test handling of AdditionalData field"""
        data = {"name": "John", "AdditionalData": {"extra": "info", "more": "data"}}
        output = StringIO()

        write_json_to_file(data, output)

        result = json.loads(output.getvalue().strip())
        assert "AdditionalData" not in result
        assert result["extra"] == "info"
        assert result["more"] == "data"
        assert result["name"] == "John"

    def test_merges_extra_json(self):
        """Test merging extra_json parameter"""
        data = {"name": "John"}
        extra = {"status": "active", "verified": True}
        output = StringIO()

        write_json_to_file(data, extra_json=extra, file=output)

        result = json.loads(output.getvalue().strip())
        assert result["name"] == "John"
        assert result["status"] == "active"
        assert result["verified"] is True

    def test_uses_custom_serializer(self):
        """Test that custom_serializer is used"""
        data = {"timestamp": datetime.datetime(2024, 6, 15, 10, 30, 45)}
        output = StringIO()

        write_json_to_file(data, output)

        result = json.loads(output.getvalue().strip())
        assert result["timestamp"] == "2024-06-15T10:30:45"


class TestAsyncFunctions:
    """Test cases for async utility functions"""

    @pytest.mark.asyncio
    async def test_log_task_executes_and_logs(self):
        """Test that log_task executes coroutine and logs timing"""

        async def sample_task():
            await asyncio.sleep(0.01)
            return "done"

        logger = logging.getLogger("test")
        logger.setLevel(logging.DEBUG)

        handler = logging.handlers.MemoryHandler(capacity=100)
        logger.addHandler(handler)

        task = TaskWrapper(name="test_task", coroutine=sample_task())

        try:
            await log_task(task, logger)
            # Task should have executed
            assert True
        finally:
            logger.removeHandler(handler)

    @pytest.mark.asyncio
    async def test_semaphore_wrapper_with_semaphore(self):
        """Test semaphore_wrapper with semaphore"""
        counter = {"value": 0}

        async def increment():
            counter["value"] += 1

        semaphore = asyncio.Semaphore(1)
        await semaphore_wrapper(increment, semaphore)

        assert counter["value"] == 1

    @pytest.mark.asyncio
    async def test_semaphore_wrapper_without_semaphore(self):
        """Test semaphore_wrapper without semaphore"""
        counter = {"value": 0}

        async def increment():
            counter["value"] += 1

        await semaphore_wrapper(increment, None)

        assert counter["value"] == 1

    @pytest.mark.asyncio
    async def test_semaphore_wrapper_respects_semaphore(self):
        """Test that semaphore actually limits concurrency"""
        semaphore = asyncio.Semaphore(1)
        running = {"count": 0, "max": 0}

        async def task_with_tracking():
            running["count"] += 1
            running["max"] = max(running["max"], running["count"])
            await asyncio.sleep(0.01)
            running["count"] -= 1

        tasks = [semaphore_wrapper(task_with_tracking, semaphore) for _ in range(5)]
        await asyncio.gather(*tasks)

        # Max concurrent should be 1 due to semaphore
        assert running["max"] == 1
