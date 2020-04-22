import pytest

from util import ensure_filename_safety


def test_preserve_alpha():
    assert ensure_filename_safety('abcdefghijklmnopqrstuvwxyz') == 'abcdefghijklmnopqrstuvwxyz'
    assert ensure_filename_safety('ABCDEFGHIJKLMNOPQRSTUVWXYZ') == 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    
def test_preserve_numeric():
    assert ensure_filename_safety('01234567890') == '01234567890'

def test_preserve_special():
    assert ensure_filename_safety('._-') == '._-'

def test_remove_special():
    assert ensure_filename_safety('`~!@#$%^&*()=+,<>/?;:"[{]}\|') == ''
    assert ensure_filename_safety("'") == ''

def test_remove_space():
    assert ensure_filename_safety('abc 123 DEF') == 'abc123DEF'
    
def test_remove_special_preserve_alphanumeric():
    assert ensure_filename_safety('a_1_!_B@2-c3#.4$xyz') == 'a_1__B2-c3.4xyz'
    assert ensure_filename_safety('&a_*1__(B2-)c3=.+4xyz?') == 'a_1__B2-c3.4xyz'
    assert ensure_filename_safety('/a\_*/1\_ _/B2-\ c3=. "4xyz ]') == 'a_1__B2-c3.4xyz'