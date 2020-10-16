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


def test_t2_star():
    assert get_sanitized_filename("t2*.dicom.zip") == "t2star.dicom.zip"
    assert get_sanitized_filename("t2 *.dicom.zip") == "t2 star.dicom.zip"
    assert get_sanitized_filename("t2_*.dicom.zip") == "t2_star.dicom.zip"

def test_preserve_carrot():
    assert get_sanitized_filename("rabbits like a ^") == "rabbits like a ^"

def test_preserve_most_printable():
    # only these characters are removed: " * / : < > ?  \ |
    assert get_sanitized_filename("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\]^_`{|}~") == "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&'()+,-.;=@[]^_`{}~"
