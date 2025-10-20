"""Tests for pure core functions in ingest.py."""

from pathlib import Path
import pandas as pd
from src.statscan import ingest


class TestSanitizeColumnNames:
    """Test column name sanitization logic."""

    def test_replaces_spaces_with_underscores(self):
        result = ingest.sanitize_column_names(['Column Name', 'Another Column'])
        assert result == ['Column_Name', 'Another_Column']

    def test_replaces_slashes_with_underscores(self):
        result = ingest.sanitize_column_names(['Date/Time', 'Value/Amount'])
        assert result == ['Date_Time', 'Value_Amount']

    def test_replaces_hyphens_with_underscores(self):
        result = ingest.sanitize_column_names(['Start-Date', 'End-Date'])
        assert result == ['Start_Date', 'End_Date']

    def test_handles_mixed_special_characters(self):
        result = ingest.sanitize_column_names(['Date/Time-Stamp', 'Value / Amount - Total'])
        assert result == ['Date_Time_Stamp', 'Value___Amount___Total']

    def test_handles_empty_list(self):
        result = ingest.sanitize_column_names([])
        assert result == []

    def test_preserves_underscores(self):
        result = ingest.sanitize_column_names(['Already_Clean', 'Another_Column'])
        assert result == ['Already_Clean', 'Another_Column']


class TestCreateFolderName:
    """Test folder name generation logic."""

    def test_basic_folder_name(self):
        result = ingest.create_folder_name(12100163, 'International Trade')
        assert result == '12100163-international-trade'

    def test_removes_special_characters(self):
        result = ingest.create_folder_name(123, 'Dataset (2024) [Final]')
        assert result == '123-dataset-2024-final'

    def test_handles_multiple_spaces(self):
        result = ingest.create_folder_name(456, 'Dataset    With    Spaces')
        assert result == '456-dataset-with-spaces'

    def test_handles_long_title(self):
        title = 'This is a very long dataset title that should be converted properly'
        result = ingest.create_folder_name(789, title)
        assert result == '789-this-is-a-very-long-dataset-title-that-should-be-converted-properly'

    def test_handles_hyphens_in_title(self):
        result = ingest.create_folder_name(999, 'Pre-Tax Income')
        assert result == '999-pre-tax-income'


class TestGenerateConversionScript:
    """Test subprocess script generation (pure function)."""

    def test_generates_valid_python_script(self):
        """Test that generated script is valid Python."""
        script = ingest.generate_conversion_script('/tmp/input.csv', '/tmp/output.parquet')

        # Should contain necessary imports
        assert 'import pandas as pd' in script
        assert 'import pyarrow.csv as pa_csv' in script
        assert 'import pyarrow.parquet as pq' in script

        # Should contain file paths
        assert '/tmp/input.csv' in script
        assert '/tmp/output.parquet' in script

        # Should contain sanitization logic (check the full chain)
        assert ".replace(' ', '_').replace('/', '_').replace('-', '_')" in script

        # Should contain string type forcing logic
        assert 'pa.string()' in script
        assert 'column_types' in script
        assert 'RecordBatch.from_arrays' in script

    def test_handles_different_paths(self):
        """Test script generation with various path formats."""
        script = ingest.generate_conversion_script(
            '/data/12100163/file.csv',
            '/data/12100163/output.parquet'
        )

        assert '/data/12100163/file.csv' in script
        assert '/data/12100163/output.parquet' in script

    def test_script_contains_all_conversion_steps(self):
        """Test that script includes all necessary streaming steps."""
        script = ingest.generate_conversion_script('input.csv', 'output.parquet')

        # Should read header with pandas
        assert 'pd.read_csv' in script
        assert 'nrows=0' in script

        # Should have all required streaming steps
        assert 'pa_csv.open_csv' in script
        assert 'pq.ParquetWriter' in script
        assert 'writer.write_batch' in script

        # Should force string types via column_types
        assert 'pa.string()' in script
        assert 'column_types' in script
        assert 'RecordBatch.from_arrays' in script

    def test_sanitization_matches_sanitize_column_names(self):
        """Test that embedded sanitization logic matches sanitize_column_names() function."""
        script = ingest.generate_conversion_script('input.csv', 'output.parquet')

        # The sanitization logic should be the same as in sanitize_column_names()
        # This ensures DRY principle - if we change sanitize_column_names(),
        # we should update the subprocess script too
        expected_logic = "[col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in original_columns]"
        assert expected_logic in script

    def test_script_uses_pathlib_path_objects(self):
        """Test that Path objects are converted to strings in script."""
        script = ingest.generate_conversion_script(
            Path('/tmp/input.csv'),
            Path('/tmp/output.parquet')
        )

        # Should handle Path objects (converted to strings via f-string)
        assert 'input.csv' in script
        assert 'output.parquet' in script

    def test_script_is_deterministic(self):
        """Test that same inputs produce same script."""
        script1 = ingest.generate_conversion_script('input.csv', 'output.parquet')
        script2 = ingest.generate_conversion_script('input.csv', 'output.parquet')

        assert script1 == script2


class TestFilterCatalog:
    """Test catalog filtering logic (the critical function)."""

    def test_removes_existing_datasets(self):
        catalog = pd.DataFrame({
            'productId': [1, 2, 3, 4, 5],
            'title': [f'Dataset {i}' for i in range(1, 6)]
        })
        existing = {2, 4}

        result = ingest.filter_catalog(catalog, existing)

        assert set(result['productId']) == {1, 3, 5}
        assert len(result) == 3

    def test_removes_invisible_datasets_by_default(self):
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['Normal', 'INVISIBLE Table', 'Another Normal']
        })

        result = ingest.filter_catalog(catalog, set())

        assert len(result) == 2
        assert 'INVISIBLE' not in result['title'].values

    def test_keeps_invisible_when_skip_invisible_false(self):
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['Normal', 'INVISIBLE Table', 'Another Normal']
        })

        result = ingest.filter_catalog(catalog, set(), skip_invisible=False)

        assert len(result) == 3

    def test_applies_limit(self):
        catalog = pd.DataFrame({
            'productId': list(range(1, 11)),
            'title': [f'Dataset {i}' for i in range(1, 11)]
        })

        result = ingest.filter_catalog(catalog, set(), limit=3)

        assert len(result) == 3
        assert list(result['productId']) == [1, 2, 3]

    def test_limit_none_returns_all(self):
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C']
        })

        result = ingest.filter_catalog(catalog, set(), limit=None)

        assert len(result) == 3

    def test_combined_filtering(self):
        """Test all filters together: existing + INVISIBLE + limit."""
        catalog = pd.DataFrame({
            'productId': [1, 2, 3, 4, 5, 6, 7],
            'title': [
                'Normal 1',          # Keep
                'INVISIBLE',         # Filter (INVISIBLE)
                'Normal 2',          # Keep
                'Normal 3',          # Filter (existing)
                'Normal 4',          # Keep
                'INVISIBLE Table',   # Filter (INVISIBLE)
                'Normal 5',          # Keep
            ]
        })
        existing = {4}  # productId 4 already exists

        result = ingest.filter_catalog(catalog, existing, skip_invisible=True, limit=2)

        # Should have: Normal 1, Normal 2 (limit=2)
        # Filtered out: INVISIBLE (2 datasets), existing (1 dataset), limit (2 more)
        assert len(result) == 2
        assert list(result['productId']) == [1, 3]

    def test_empty_catalog(self):
        """Test that empty catalog returns empty result."""
        catalog = pd.DataFrame({'productId': [], 'title': []})

        result = ingest.filter_catalog(catalog, set(), skip_invisible=False)

        assert len(result) == 0


class TestFormatDisplayTitle:
    """Test display title formatting logic."""

    def test_short_title_no_truncation(self):
        """Test that short titles are not truncated."""
        result = ingest.format_display_title(12100163, 'International Trade')

        assert result == '[12100163] International Trade'

    def test_long_title_gets_truncated(self):
        """Test that titles over 50 chars are truncated."""
        long_title = 'This is a very long dataset title that exceeds fifty characters easily'
        result = ingest.format_display_title(123, long_title)

        assert result.startswith('[123]')
        assert result.endswith('...')
        assert len(result) <= 55 + len('[123] ')  # 50 chars + "[123] " + "..."

    def test_exactly_50_chars_no_truncation(self):
        """Test title exactly 50 chars long."""
        title_50 = 'A' * 50
        result = ingest.format_display_title(456, title_50)

        assert result == f'[456] {title_50}'
        assert not result.endswith('...')

    def test_51_chars_gets_truncated(self):
        """Test that 51 char title gets truncated."""
        title_51 = 'A' * 51
        result = ingest.format_display_title(789, title_51)

        assert result.endswith('...')
        assert 'A' * 50 in result

    def test_custom_max_len(self):
        """Test custom truncation length."""
        long_title = 'A' * 100
        result = ingest.format_display_title(999, long_title, max_len=20)

        assert result == f'[999] {"A" * 20}...'

    def test_empty_title(self):
        """Test empty title edge case."""
        result = ingest.format_display_title(111, '')

        assert result == '[111] '

    def test_unicode_title(self):
        """Test title with unicode characters."""
        result = ingest.format_display_title(222, 'Données économiques 中文')

        assert result == '[222] Données économiques 中文'


class TestFormatErrorMessage:
    """Test error message formatting logic."""

    def test_short_error_message(self):
        """Test that short error messages are returned as-is."""
        error = ValueError('Short error')
        result = ingest.format_error_message(error)

        assert result == 'Short error'

    def test_long_error_message_returns_type(self):
        """Test that long error messages return exception type."""
        long_msg = 'A' * 100
        error = ValueError(long_msg)
        result = ingest.format_error_message(error)

        assert result == 'ValueError'

    def test_exactly_50_chars_not_truncated(self):
        """Test message exactly 50 chars long."""
        msg_50 = 'A' * 50
        error = RuntimeError(msg_50)
        result = ingest.format_error_message(error)

        assert result == msg_50

    def test_51_chars_returns_type(self):
        """Test that 51 char message returns type."""
        msg_51 = 'A' * 51
        error = KeyError(msg_51)
        result = ingest.format_error_message(error)

        assert result == 'KeyError'

    def test_custom_max_len(self):
        """Test custom truncation length."""
        error = Exception('This is a somewhat long error message')
        result = ingest.format_error_message(error, max_len=10)

        assert result == 'Exception'

    def test_empty_error_message(self):
        """Test exception with empty message."""
        error = ValueError('')
        result = ingest.format_error_message(error)

        assert result == ''

    def test_various_exception_types(self):
        """Test different exception types."""
        errors = [
            (ValueError('test'), 'test'),
            (TypeError('another'), 'another'),
            (RuntimeError('A' * 60), 'RuntimeError'),
            (KeyError('B' * 60), 'KeyError'),
        ]

        for error, expected in errors:
            result = ingest.format_error_message(error)
            assert result == expected


class TestCalculateDownloadProgress:
    """Test download progress calculation logic."""

    def test_zero_downloaded_zero_total(self):
        """Test edge case of zero total."""
        result = ingest.calculate_download_progress(0, 0)
        assert result == 0

    def test_negative_total(self):
        """Test edge case of negative total."""
        result = ingest.calculate_download_progress(100, -1)
        assert result == 0

    def test_zero_downloaded_positive_total(self):
        """Test 0% progress."""
        result = ingest.calculate_download_progress(0, 1000)
        assert result == 0

    def test_half_downloaded(self):
        """Test 50% progress."""
        result = ingest.calculate_download_progress(500, 1000)
        assert result == 50

    def test_full_downloaded(self):
        """Test 100% progress."""
        result = ingest.calculate_download_progress(1000, 1000)
        assert result == 100

    def test_fractional_progress_rounds_down(self):
        """Test that fractional percentages are truncated."""
        result = ingest.calculate_download_progress(333, 1000)
        assert result == 33  # 33.3% rounds down

    def test_large_numbers(self):
        """Test with large byte counts (GBs)."""
        result = ingest.calculate_download_progress(1500000000, 3000000000)
        assert result == 50

    def test_various_percentages(self):
        """Test various percentage calculations."""
        test_cases = [
            (10, 100, 10),
            (25, 100, 25),
            (99, 100, 99),
            (1, 100, 1),
            (100, 1000, 10),
        ]

        for downloaded, total, expected in test_cases:
            result = ingest.calculate_download_progress(downloaded, total)
            assert result == expected


class TestShouldPrintProgress:
    """Test progress printing decision logic."""

    def test_first_print_at_interval(self):
        """Test that first interval triggers print."""
        result = ingest.should_print_progress(10, -1, interval=10)
        assert result is True

    def test_no_print_before_interval(self):
        """Test that we don't print before reaching interval."""
        result = ingest.should_print_progress(5, 0, interval=10)
        assert result is False

    def test_print_at_exactly_interval(self):
        """Test print at exactly the interval boundary."""
        result = ingest.should_print_progress(20, 10, interval=10)
        assert result is True

    def test_no_print_just_before_interval(self):
        """Test no print just before interval."""
        result = ingest.should_print_progress(19, 10, interval=10)
        assert result is False

    def test_print_past_interval(self):
        """Test print when we've exceeded interval."""
        result = ingest.should_print_progress(25, 10, interval=10)
        assert result is True

    def test_custom_interval(self):
        """Test with custom interval (5%)."""
        test_cases = [
            (5, 0, True),   # First 5%
            (4, 0, False),  # Not yet
            (10, 5, True),  # Next 5%
            (14, 10, False),  # Not yet
            (15, 10, True),  # Next interval
        ]

        for current, last, expected in test_cases:
            result = ingest.should_print_progress(current, last, interval=5)
            assert result == expected

    def test_large_jump_in_progress(self):
        """Test when progress jumps multiple intervals."""
        result = ingest.should_print_progress(50, 10, interval=10)
        assert result is True

    def test_zero_progress(self):
        """Test at start (0%) - should not print yet."""
        result = ingest.should_print_progress(0, -1, interval=10)
        assert result is False  # 0 < -1 + 10, so not ready to print
