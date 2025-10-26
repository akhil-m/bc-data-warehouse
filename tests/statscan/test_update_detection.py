"""Tests for update detection pure functions."""

from datetime import datetime, timedelta
import pandas as pd
import pytest
from src.statscan import update_detection


class TestParseFrequencyToDays:
    """Test frequency string to days conversion."""

    def test_daily(self):
        """Test Daily frequency returns 1 day."""
        assert update_detection.parse_frequency_to_days("Daily") == 1

    def test_weekly(self):
        """Test Weekly frequency returns 7 days."""
        assert update_detection.parse_frequency_to_days("Weekly") == 7

    def test_biweekly(self):
        """Test Bi-weekly frequency returns 14 days."""
        assert update_detection.parse_frequency_to_days("Bi-weekly") == 14

    def test_monthly(self):
        """Test Monthly frequency returns 30 days."""
        assert update_detection.parse_frequency_to_days("Monthly") == 30

    def test_quarterly(self):
        """Test Quarterly frequency returns 90 days."""
        assert update_detection.parse_frequency_to_days("Quarterly") == 90

    def test_semiannual(self):
        """Test Semi-annual frequency returns 180 days."""
        assert update_detection.parse_frequency_to_days("Semi-annual") == 180

    def test_annual(self):
        """Test Annual frequency returns 365 days."""
        assert update_detection.parse_frequency_to_days("Annual") == 365

    def test_occasional(self):
        """Test Occasional frequency returns 180 days."""
        assert update_detection.parse_frequency_to_days("Occasional") == 180

    def test_unknown_frequency_default(self):
        """Test unknown frequency returns default 180 days."""
        assert update_detection.parse_frequency_to_days("Unknown") == 180
        assert update_detection.parse_frequency_to_days("") == 180
        assert update_detection.parse_frequency_to_days("InvalidFreq") == 180


class TestShouldCheckForUpdate:
    """Test update detection logic based on frequency and time."""

    def test_monthly_not_ready(self):
        """Test monthly dataset ingested 15 days ago is not ready."""
        current = datetime(2024, 2, 15)
        last_ingestion = datetime(2024, 1, 31)  # 15 days ago
        assert update_detection.should_check_for_update("Monthly", last_ingestion, current) is False

    def test_monthly_ready(self):
        """Test monthly dataset ingested 35 days ago is ready."""
        current = datetime(2024, 3, 5)
        last_ingestion = datetime(2024, 1, 30)  # 35 days ago
        assert update_detection.should_check_for_update("Monthly", last_ingestion, current) is True

    def test_monthly_exactly_30_days(self):
        """Test monthly dataset ingested exactly 30 days ago is ready."""
        current = datetime(2024, 3, 1)
        last_ingestion = datetime(2024, 1, 31)  # Exactly 30 days ago
        assert update_detection.should_check_for_update("Monthly", last_ingestion, current) is True

    def test_quarterly_not_ready(self):
        """Test quarterly dataset ingested 60 days ago is not ready."""
        current = datetime(2024, 3, 15)
        last_ingestion = datetime(2024, 1, 15)  # 60 days ago
        assert update_detection.should_check_for_update("Quarterly", last_ingestion, current) is False

    def test_quarterly_ready(self):
        """Test quarterly dataset ingested 95 days ago is ready."""
        current = datetime(2024, 4, 20)
        last_ingestion = datetime(2024, 1, 15)  # 95+ days ago
        assert update_detection.should_check_for_update("Quarterly", last_ingestion, current) is True

    def test_annual_not_ready(self):
        """Test annual dataset ingested 200 days ago is not ready."""
        current = datetime(2024, 8, 1)
        last_ingestion = datetime(2024, 1, 15)  # ~200 days ago
        assert update_detection.should_check_for_update("Annual", last_ingestion, current) is False

    def test_annual_ready(self):
        """Test annual dataset ingested 370 days ago is ready."""
        current = datetime(2025, 1, 20)
        last_ingestion = datetime(2024, 1, 15)  # 370+ days ago
        assert update_detection.should_check_for_update("Annual", last_ingestion, current) is True

    def test_daily_ready_after_one_day(self):
        """Test daily dataset ingested yesterday is ready."""
        current = datetime(2024, 1, 2, 10, 0, 0)
        last_ingestion = datetime(2024, 1, 1, 9, 0, 0)  # >1 day ago
        assert update_detection.should_check_for_update("Daily", last_ingestion, current) is True


class TestIdentifyDatasetsForProcessing:
    """Test identification of new and update-due datasets."""

    def test_first_run_all_new(self):
        """Test first run with no existing catalog returns all as new."""
        fresh = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['Dataset A', 'Dataset B', 'Dataset C'],
            'frequency': ['Monthly', 'Quarterly', 'Annual']
        })
        existing = pd.DataFrame()  # Empty

        current = datetime(2024, 1, 1)
        result = update_detection.identify_datasets_for_processing(fresh, existing, current)

        assert len(result) == 3
        assert set(result['productId']) == {'A', 'B', 'C'}
        assert all(result['reason'] == 'new')

    def test_no_new_no_updates_due(self):
        """Test no processing needed when all datasets are fresh."""
        fresh = pd.DataFrame({
            'productId': ['A', 'B'],
            'title': ['Dataset A', 'Dataset B'],
            'frequency': ['Monthly', 'Quarterly']
        })
        existing = pd.DataFrame({
            'productId': ['A', 'B'],
            'last_ingestion_date': [
                datetime(2024, 1, 25),  # 5 days ago
                datetime(2024, 1, 20)   # 10 days ago
            ]
        })

        current = datetime(2024, 1, 30)
        result = update_detection.identify_datasets_for_processing(fresh, existing, current)

        assert len(result) == 0

    def test_monthly_update_due(self):
        """Test monthly dataset ingested 35 days ago appears in results."""
        fresh = pd.DataFrame({
            'productId': ['A'],
            'title': ['Monthly Dataset'],
            'frequency': ['Monthly']
        })
        existing = pd.DataFrame({
            'productId': ['A'],
            'last_ingestion_date': [datetime(2024, 1, 1)]  # 35+ days ago
        })

        current = datetime(2024, 2, 5)
        result = update_detection.identify_datasets_for_processing(fresh, existing, current)

        assert len(result) == 1
        assert result.iloc[0]['productId'] == 'A'
        assert result.iloc[0]['reason'] == 'update_due'

    def test_new_and_updates_mixed(self):
        """Test mix of new datasets and updates."""
        fresh = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['Dataset A', 'Dataset B', 'Dataset C'],
            'frequency': ['Monthly', 'Monthly', 'Quarterly']
        })
        existing = pd.DataFrame({
            'productId': ['A', 'C'],
            'last_ingestion_date': [
                datetime(2024, 1, 1),   # A: 40 days ago (monthly -> update due)
                datetime(2024, 1, 20)   # C: 20 days ago (quarterly -> not due)
            ]
        })

        current = datetime(2024, 2, 10)
        result = update_detection.identify_datasets_for_processing(fresh, existing, current)

        assert len(result) == 2
        assert 'A' in result['productId'].values  # Update due
        assert 'B' in result['productId'].values  # New
        assert 'C' not in result['productId'].values  # Not due yet

        a_row = result[result['productId'] == 'A'].iloc[0]
        assert a_row['reason'] == 'update_due'

        b_row = result[result['productId'] == 'B'].iloc[0]
        assert b_row['reason'] == 'new'

    def test_null_ingestion_date_treated_as_new(self):
        """Test dataset with NaT last_ingestion_date is treated as new."""
        fresh = pd.DataFrame({
            'productId': ['A'],
            'title': ['Dataset A'],
            'frequency': ['Monthly']
        })
        existing = pd.DataFrame({
            'productId': ['A'],
            'last_ingestion_date': [pd.NaT]
        })

        current = datetime(2024, 1, 1)
        result = update_detection.identify_datasets_for_processing(fresh, existing, current)

        assert len(result) == 1
        assert result.iloc[0]['productId'] == 'A'
        assert result.iloc[0]['reason'] == 'new'

    def test_multiple_frequencies_correct_intervals(self):
        """Test different frequencies use correct update intervals."""
        fresh = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['Daily', 'Monthly', 'Annual'],
            'frequency': ['Daily', 'Monthly', 'Annual']
        })
        existing = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'last_ingestion_date': [
                datetime(2024, 1, 1),   # A: 2 days ago (daily -> update due)
                datetime(2024, 1, 1),   # B: 2 days ago (monthly -> not due)
                datetime(2024, 1, 1)    # C: 2 days ago (annual -> not due)
            ]
        })

        current = datetime(2024, 1, 3)
        result = update_detection.identify_datasets_for_processing(fresh, existing, current)

        # Only daily dataset should need update
        assert len(result) == 1
        assert result.iloc[0]['productId'] == 'A'
        assert result.iloc[0]['reason'] == 'update_due'

    def test_result_includes_all_required_columns(self):
        """Test result DataFrame has all required columns."""
        fresh = pd.DataFrame({
            'productId': ['A'],
            'title': ['Dataset A'],
            'frequency': ['Monthly']
        })
        existing = pd.DataFrame()

        current = datetime(2024, 1, 1)
        result = update_detection.identify_datasets_for_processing(fresh, existing, current)

        assert 'productId' in result.columns
        assert 'title' in result.columns
        assert 'frequency' in result.columns
        assert 'reason' in result.columns


class TestApplyLimitToNewDatasets:
    """Test limiting new datasets while preserving all updates."""

    def test_no_limit_returns_all_datasets(self):
        """Test that None limit returns all datasets unchanged."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['Dataset A', 'Dataset B', 'Dataset C'],
            'frequency': ['Monthly', 'Quarterly', 'Annual'],
            'reason': ['new', 'update_due', 'new']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, None)

        assert len(result) == 3
        assert set(result['productId']) == {'A', 'B', 'C'}

    def test_limit_applies_only_to_new_datasets(self):
        """Test that limit applies only to new datasets, not updates."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B', 'C', 'D', 'E'],
            'title': ['A', 'B', 'C', 'D', 'E'],
            'frequency': ['Monthly'] * 5,
            'reason': ['new', 'new', 'new', 'update_due', 'update_due']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, limit=2)

        # Should have 2 new + 2 updates = 4 total
        assert len(result) == 4
        assert (result['reason'] == 'new').sum() == 2
        assert (result['reason'] == 'update_due').sum() == 2

    def test_limit_takes_first_n_new_datasets(self):
        """Test that limit takes first N new datasets (respects order)."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B', 'C', 'D'],
            'title': ['A', 'B', 'C', 'D'],
            'frequency': ['Monthly'] * 4,
            'reason': ['new', 'new', 'new', 'update_due']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, limit=2)

        new_datasets = result[result['reason'] == 'new']
        assert list(new_datasets['productId']) == ['A', 'B']  # First 2

    def test_all_updates_preserved_regardless_of_limit(self):
        """Test that all update_due datasets are included even with small limit."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B', 'C', 'D', 'E'],
            'title': ['A', 'B', 'C', 'D', 'E'],
            'frequency': ['Monthly'] * 5,
            'reason': ['new', 'update_due', 'update_due', 'update_due', 'new']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, limit=1)

        # Should have 1 new + 3 updates = 4 total
        assert len(result) == 4
        assert (result['reason'] == 'new').sum() == 1
        assert (result['reason'] == 'update_due').sum() == 3

    def test_limit_larger_than_new_count(self):
        """Test that limit larger than new count includes all datasets."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['A', 'B', 'C'],
            'frequency': ['Monthly'] * 3,
            'reason': ['new', 'new', 'update_due']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, limit=10)

        assert len(result) == 3
        assert set(result['productId']) == {'A', 'B', 'C'}

    def test_zero_limit(self):
        """Test that limit=0 excludes all new datasets but keeps updates."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['A', 'B', 'C'],
            'frequency': ['Monthly'] * 3,
            'reason': ['new', 'new', 'update_due']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, limit=0)

        assert len(result) == 1
        assert result.iloc[0]['productId'] == 'C'
        assert result.iloc[0]['reason'] == 'update_due'

    def test_only_new_datasets_with_limit(self):
        """Test limiting when there are only new datasets."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['A', 'B', 'C'],
            'frequency': ['Monthly'] * 3,
            'reason': ['new', 'new', 'new']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, limit=2)

        assert len(result) == 2
        assert all(result['reason'] == 'new')

    def test_only_updates_with_limit(self):
        """Test behavior when there are only updates (limit irrelevant)."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B', 'C'],
            'title': ['A', 'B', 'C'],
            'frequency': ['Monthly'] * 3,
            'reason': ['update_due', 'update_due', 'update_due']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, limit=1)

        # All 3 updates should be included
        assert len(result) == 3
        assert all(result['reason'] == 'update_due')

    def test_preserves_all_columns(self):
        """Test that all columns from input DataFrame are preserved."""
        datasets = pd.DataFrame({
            'productId': ['A', 'B'],
            'title': ['Dataset A', 'Dataset B'],
            'frequency': ['Monthly', 'Quarterly'],
            'reason': ['new', 'update_due'],
            'extra_column': ['value1', 'value2']
        })

        result = update_detection.apply_limit_to_new_datasets(datasets, limit=1)

        assert 'extra_column' in result.columns
        assert set(result.columns) == set(datasets.columns)
