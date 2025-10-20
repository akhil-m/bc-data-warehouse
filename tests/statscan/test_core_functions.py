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
        assert 'import pyarrow.csv as pa_csv' in script
        assert 'import pyarrow.parquet as pq' in script

        # Should contain file paths
        assert '/tmp/input.csv' in script
        assert '/tmp/output.parquet' in script

        # Should contain sanitization logic (check the full chain)
        assert ".replace(' ', '_').replace('/', '_').replace('-', '_')" in script

    def test_handles_different_paths(self):
        """Test script generation with various path formats."""
        script = ingest.generate_conversion_script(
            '/data/12100163/file.csv',
            '/data/12100163/output.parquet'
        )

        assert '/data/12100163/file.csv' in script
        assert '/data/12100163/output.parquet' in script

    def test_script_contains_all_conversion_steps(self):
        """Test that script includes all necessary steps."""
        script = ingest.generate_conversion_script('input.csv', 'output.parquet')

        # Should have all required steps in order
        assert 'pa_csv.read_csv' in script
        assert 'table.column_names' in script
        assert 'table.rename_columns' in script
        assert 'pq.write_table' in script

    def test_sanitization_matches_sanitize_column_names(self):
        """Test that embedded sanitization logic matches sanitize_column_names() function."""
        script = ingest.generate_conversion_script('input.csv', 'output.parquet')

        # The sanitization logic should be the same as in sanitize_column_names()
        # This ensures DRY principle - if we change sanitize_column_names(),
        # we should update the subprocess script too
        expected_logic = "[col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in columns]"
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
