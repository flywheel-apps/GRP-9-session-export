from util import quote_numeric_string
import pytest


def test_quote_numeric_str():
    assert quote_numeric_string(9860) == '"9860"'
    assert quote_numeric_string('9860') == '"9860"'
    assert quote_numeric_string('nine86zero') == 'nine86zero'
    assert quote_numeric_string('9860zero') == '9860zero'

