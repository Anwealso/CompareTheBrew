"""
Tier 1 unit tests for the pure helper functions used during parsing:
- RetailerProcessor.clean_numeric / parse_volume / is_zero_alc
  (scraping/processor.py)
- calculate_score (db/databaseHandler.py)

No network, no DB, milliseconds. BWSProcessor is used as a concrete
RetailerProcessor stand-in purely to exercise the shared base-class
helpers; none of these tests touch BWS-specific parsing logic.
"""
import pytest

from db.databaseHandler import calculate_score
from scraping.bws_processor import BWSProcessor


@pytest.fixture
def proc():
    return BWSProcessor()


# ---------------------------------------------------------------------
# parse_volume
# ---------------------------------------------------------------------


def test_parse_volume_ml(proc):
    assert proc.parse_volume("Some Beer 375ml Can") == 375.0


def test_parse_volume_litres_converted_to_ml(proc):
    assert proc.parse_volume("Some Wine 1.5L Bottle") == 1500.0


def test_parse_volume_from_pack_string(proc):
    # First ml/l match in the string wins.
    assert proc.parse_volume("Pack of 24 x 375mL") == 375.0


def test_parse_volume_unparseable_defaults_to_zero(proc):
    assert proc.parse_volume("no volume mentioned here") == 0.0


# ---------------------------------------------------------------------
# is_zero_alc
# ---------------------------------------------------------------------


def test_is_zero_alc_boundary_at_half_percent_is_true(proc):
    assert proc.is_zero_alc(0.5) is True


def test_is_zero_alc_just_above_boundary_is_false(proc):
    assert proc.is_zero_alc(0.6) is False


def test_is_zero_alc_zero_percent_is_true(proc):
    assert proc.is_zero_alc(0.0) is True


def test_is_zero_alc_none_treated_as_zero_is_true(proc):
    assert proc.is_zero_alc(None) is True


def test_is_zero_alc_non_numeric_is_false(proc):
    # Can't parse it as a float, so it can't be asserted zero-alc.
    assert proc.is_zero_alc("abc") is False


# ---------------------------------------------------------------------
# clean_numeric
# ---------------------------------------------------------------------


def test_clean_numeric_strips_percent_sign(proc):
    assert proc.clean_numeric("5.2%") == 5.2


def test_clean_numeric_strips_dollar_sign(proc):
    assert proc.clean_numeric("$12.50") == 12.5


def test_clean_numeric_strips_thousands_comma(proc):
    assert proc.clean_numeric("1,234") == 1234.0


def test_clean_numeric_accepts_float_and_int(proc):
    assert proc.clean_numeric(5) == 5.0
    assert proc.clean_numeric(5.5) == 5.5


def test_clean_numeric_bad_input_defaults_to_zero(proc):
    assert proc.clean_numeric("abc") == 0.0


def test_clean_numeric_none_defaults_to_zero(proc):
    assert proc.clean_numeric(None) == 0.0


# ---------------------------------------------------------------------
# calculate_score
# ---------------------------------------------------------------------


def test_calculate_score_normal_case(proc):
    assert calculate_score(10, 2) == 5.0


def test_calculate_score_zero_std_drinks_is_none(proc):
    assert calculate_score(10, 0) is None


def test_calculate_score_pack_qty_multiplies_std_drinks(proc):
    # price / (std * qty) = 10 / (2 * 3)
    assert calculate_score(10, 2, 3) == pytest.approx(10 / 6)


def test_calculate_score_qty_below_one_is_clamped_to_one(proc):
    # qty=0 clamps to 1, so this behaves like calculate_score(10, 2).
    assert calculate_score(10, 2, 0) == 5.0


def test_calculate_score_non_numeric_price_is_none(proc):
    assert calculate_score("abc", 2) is None


def test_calculate_score_non_numeric_std_drinks_is_none(proc):
    assert calculate_score(10, "abc") is None


def test_calculate_score_zero_price_is_none(proc):
    assert calculate_score(0, 2) is None


def test_calculate_score_none_price_is_none(proc):
    assert calculate_score(None, 2) is None
