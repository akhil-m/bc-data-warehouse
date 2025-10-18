"""Tests for pure core functions in ingest_all.py."""

import pandas as pd
import ingest_all


class TestSanitizeColumnNames:
    """Test column name sanitization logic."""

    def test_replaces_spaces_with_underscores(self):
        result = ingest_all.sanitize_column_names(['Column Name', 'Another Column'])
        assert result == ['Column_Name', 'Another_Column']

    def test_replaces_slashes_with_underscores(self):
        result = ingest_all.sanitize_column_names(['Date/Time', 'Value/Amount'])
        assert result == ['Date_Time', 'Value_Amount']

    def test_replaces_hyphens_with_underscores(self):
        result = ingest_all.sanitize_column_names(['Start-Date', 'End-Date'])
        assert result == ['Start_Date', 'End_Date']

    def test_handles_mixed_special_characters(self):
        result = ingest_all.sanitize_column_names(['Date/Time-Stamp', 'Value / Amount - Total'])
        assert result == ['Date_Time_Stamp', 'Value___Amount___Total']

    def test_handles_empty_list(self):
        result = ingest_all.sanitize_column_names([])
        assert result == []

    def test_preserves_underscores(self):
        result = ingest_all.sanitize_column_names(['Already_Clean', 'Another_Column'])
        assert result == ['Already_Clean', 'Another_Column']


class TestCreateFolderName:
    """Test folder name generation logic."""

    def test_basic_folder_name(self):
        result = ingest_all.create_folder_name(12100163, 'International Trade')
        assert result == '12100163-international-trade'

    def test_removes_special_characters(self):
        result = ingest_all.create_folder_name(123, 'Dataset (2024) [Final]')
        assert result == '123-dataset-2024-final'

    def test_handles_multiple_spaces(self):
        result = ingest_all.create_folder_name(456, 'Dataset    With    Spaces')
        assert result == '456-dataset-with-spaces'

    def test_handles_long_title(self):
        title = 'This is a very long dataset title that should be converted properly'
        result = ingest_all.create_folder_name(789, title)
        assert result == '789-this-is-a-very-long-dataset-title-that-should-be-converted-properly'

    def test_handles_hyphens_in_title(self):
        result = ingest_all.create_folder_name(999, 'Pre-Tax Income')
        assert result == '999-pre-tax-income'


class TestShouldDownload:
    """Test file size limit checking."""

    def test_allows_file_under_limit(self):
        # 100MB file, 200MB limit
        assert ingest_all.should_download(100 * 1e6, 200) is True

    def test_allows_file_at_exact_limit(self):
        # 100MB file, 100MB limit
        assert ingest_all.should_download(100 * 1e6, 100) is True

    def test_rejects_file_over_limit(self):
        # 300MB file, 200MB limit
        assert ingest_all.should_download(300 * 1e6, 200) is False

    def test_allows_small_file(self):
        # 1MB file, 5000MB limit
        assert ingest_all.should_download(1 * 1e6, 5000) is True

    def test_rejects_huge_file(self):
        # 10GB file, 5000MB limit
        assert ingest_all.should_download(10 * 1e9, 5000) is False


class TestFilterCatalog:
    """Test catalog filtering logic (the critical function)."""

    def test_removes_existing_datasets(self):
        catalog = pd.DataFrame({
            'productId': [1, 2, 3, 4, 5],
            'title': [f'Dataset {i}' for i in range(1, 6)]
        })
        existing = {2, 4}

        result = ingest_all.filter_catalog(catalog, existing)

        assert set(result['productId']) == {1, 3, 5}
        assert len(result) == 3

    def test_removes_invisible_datasets_by_default(self):
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['Normal', 'INVISIBLE Table', 'Another Normal']
        })

        result = ingest_all.filter_catalog(catalog, set())

        assert len(result) == 2
        assert 'INVISIBLE' not in result['title'].values

    def test_keeps_invisible_when_skip_invisible_false(self):
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['Normal', 'INVISIBLE Table', 'Another Normal']
        })

        result = ingest_all.filter_catalog(catalog, set(), skip_invisible=False)

        assert len(result) == 3

    def test_applies_limit(self):
        catalog = pd.DataFrame({
            'productId': list(range(1, 11)),
            'title': [f'Dataset {i}' for i in range(1, 11)]
        })

        result = ingest_all.filter_catalog(catalog, set(), limit=3)

        assert len(result) == 3
        assert list(result['productId']) == [1, 2, 3]

    def test_limit_none_returns_all(self):
        catalog = pd.DataFrame({
            'productId': [1, 2, 3],
            'title': ['A', 'B', 'C']
        })

        result = ingest_all.filter_catalog(catalog, set(), limit=None)

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

        result = ingest_all.filter_catalog(catalog, existing, skip_invisible=True, limit=2)

        # Should have: Normal 1, Normal 2 (limit=2)
        # Filtered out: INVISIBLE (2 datasets), existing (1 dataset), limit (2 more)
        assert len(result) == 2
        assert list(result['productId']) == [1, 3]

    def test_empty_catalog(self):
        """Test that empty catalog returns empty result."""
        catalog = pd.DataFrame({'productId': [], 'title': []})

        result = ingest_all.filter_catalog(catalog, set(), skip_invisible=False)

        assert len(result) == 0
