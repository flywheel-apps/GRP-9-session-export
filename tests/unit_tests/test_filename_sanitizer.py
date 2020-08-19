import pytest

from util import get_sanitized_filename


def test_preserve_alpha():
    assert get_sanitized_filename('abcdefghijklmnopqrstuvwxyz') == 'abcdefghijklmnopqrstuvwxyz'
    assert get_sanitized_filename('ABCDEFGHIJKLMNOPQRSTUVWXYZ') == 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    
def test_preserve_numeric():
    assert get_sanitized_filename('01234567890') == '01234567890'

def test_preserve_special():
    assert get_sanitized_filename('._-') == '._-'

def test_remove_special():
    assert get_sanitized_filename("_a*b:c<d>e%f/(g)h+i_0.txt") == '_abcde%f(g)h+i_0.txt'
    assert get_sanitized_filename('fi:l*e/p"a?t>h|.t<xt') == 'filepath.txt'

