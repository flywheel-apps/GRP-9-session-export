import pytest

from util import quote_numeric_string


def test_quote_numeric_str():
    assert quote_numeric_string(9860) == '"9860"'
    assert quote_numeric_string('9860') == '"9860"'
    assert quote_numeric_string('"9860"') == '"9860"'
    assert quote_numeric_string('nine86zero') == 'nine86zero'
    assert quote_numeric_string('9860zero') == '9860zero'
    assert quote_numeric_string('98.60') == '"98.60"'


def test_query_strings():
    assert 'code={}'.format(quote_numeric_string('9860')) == 'code="9860"'
    assert 'code={}'.format(quote_numeric_string('"9860"')) == 'code="9860"'
    assert 'code={}'.format(quote_numeric_string('nine86zero')) == 'code=nine86zero'
    assert 'code={}'.format(quote_numeric_string('98.60')) == 'code="98.60"'
