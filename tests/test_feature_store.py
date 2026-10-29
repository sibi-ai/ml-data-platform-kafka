"""Tests for feature store point-in-time correctness."""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import pandas as pd


def test_schema_validation_valid():
    from src.quality.validator import SchemaValidator
    v = SchemaValidator()
    record = {"entity_id": "u1", "ts": 1700000000.0,
               "feature_a": 1.0, "feature_b": 2.0, "feature_c": 3.0}
    assert v.validate(record) is True


def test_schema_validation_missing_field():
    from src.quality.validator import SchemaValidator
    v = SchemaValidator()
    record = {"entity_id": "u1", "ts": 1700000000.0, "feature_a": 1.0}
    assert v.validate(record) is False


def test_schema_validation_wrong_type():
    from src.quality.validator import SchemaValidator
    v = SchemaValidator()
    record = {"entity_id": "u1", "ts": "not-a-float",
               "feature_a": 1.0, "feature_b": 2.0, "feature_c": 3.0}
    assert v.validate(record) is False
