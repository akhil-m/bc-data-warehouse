"""Tests for catalog discovery functions."""

import pytest
from src.statscan import discover


class TestExtractCatalogMetadata:
    """Test catalog metadata extraction (pure function)."""

    def test_extracts_basic_fields(self):
        """Test extraction of core metadata fields."""
        cubes = [
            {
                'productId': 12100163,
                'cubeTitleEn': 'International Trade',
                'subjectEn': 'Economics',
                'frequencyCode': 'M',
                'releaseTime': '2024-01-01T09:00:00',
                'dimensions': [{'id': 1}, {'id': 2}, {'id': 3}],
                'nbDatapointsCube': 100000
            }
        ]

        result = discover.extract_catalog_metadata(cubes)

        assert len(result) == 1
        assert result[0]['productId'] == 12100163
        assert result[0]['title'] == 'International Trade'
        assert result[0]['subject'] == 'Economics'
        assert result[0]['frequency'] == 'M'
        assert result[0]['releaseTime'] == '2024-01-01T09:00:00'
        assert result[0]['dimensions'] == 3
        assert result[0]['nbDatapoints'] == 100000

    def test_handles_multiple_cubes(self):
        """Test processing multiple datasets."""
        cubes = [
            {
                'productId': 1,
                'cubeTitleEn': 'Dataset 1',
                'subjectEn': 'Economics',
                'frequencyCode': 'M',
                'releaseTime': '2024-01-01',
                'dimensions': [{'id': 1}],
                'nbDatapointsCube': 1000
            },
            {
                'productId': 2,
                'cubeTitleEn': 'Dataset 2',
                'subjectEn': 'Immigration',
                'frequencyCode': 'A',
                'releaseTime': '2024-02-01',
                'dimensions': [{'id': 1}, {'id': 2}],
                'nbDatapointsCube': 2000
            }
        ]

        result = discover.extract_catalog_metadata(cubes)

        assert len(result) == 2
        assert result[0]['productId'] == 1
        assert result[1]['productId'] == 2
        assert result[0]['dimensions'] == 1
        assert result[1]['dimensions'] == 2

    def test_handles_missing_fields(self):
        """Test graceful handling of missing optional fields."""
        cubes = [
            {
                'productId': 123,
                # Missing cubeTitleEn, subjectEn, etc.
            }
        ]

        result = discover.extract_catalog_metadata(cubes)

        assert len(result) == 1
        assert result[0]['productId'] == 123
        assert result[0]['title'] is None
        assert result[0]['subject'] is None
        assert result[0]['frequency'] is None
        assert result[0]['releaseTime'] is None
        assert result[0]['dimensions'] == 0  # Empty dimensions list
        assert result[0]['nbDatapoints'] is None

    def test_handles_empty_dimensions(self):
        """Test dimension counting with empty list."""
        cubes = [
            {
                'productId': 456,
                'cubeTitleEn': 'Test',
                'dimensions': []
            }
        ]

        result = discover.extract_catalog_metadata(cubes)

        assert result[0]['dimensions'] == 0

    def test_handles_missing_dimensions_key(self):
        """Test dimension counting when dimensions key is absent."""
        cubes = [
            {
                'productId': 789,
                'cubeTitleEn': 'Test',
                # No dimensions key at all
            }
        ]

        result = discover.extract_catalog_metadata(cubes)

        # len(cube.get('dimensions', [])) should default to empty list
        assert result[0]['dimensions'] == 0

    def test_handles_empty_cube_list(self):
        """Test that empty input returns empty output."""
        cubes = []

        result = discover.extract_catalog_metadata(cubes)

        assert result == []
        assert len(result) == 0

    def test_preserves_order(self):
        """Test that cube order is preserved in output."""
        cubes = [
            {'productId': 3, 'cubeTitleEn': 'Third'},
            {'productId': 1, 'cubeTitleEn': 'First'},
            {'productId': 2, 'cubeTitleEn': 'Second'}
        ]

        result = discover.extract_catalog_metadata(cubes)

        assert [r['productId'] for r in result] == [3, 1, 2]
        assert [r['title'] for r in result] == ['Third', 'First', 'Second']

    def test_handles_large_dimension_count(self):
        """Test datasets with many dimensions (StatsCan can have 50+)."""
        cubes = [
            {
                'productId': 999,
                'cubeTitleEn': 'Large Dataset',
                'dimensions': [{'id': i} for i in range(60)]
            }
        ]

        result = discover.extract_catalog_metadata(cubes)

        assert result[0]['dimensions'] == 60

    def test_all_fields_present_in_output(self):
        """Test that all expected fields are in output dict."""
        cubes = [{'productId': 1}]

        result = discover.extract_catalog_metadata(cubes)

        expected_keys = {
            'productId', 'title', 'subject', 'frequency',
            'releaseTime', 'dimensions', 'nbDatapoints'
        }
        assert set(result[0].keys()) == expected_keys
