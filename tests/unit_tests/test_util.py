from flywheel import ApiException
from util import *


def test_quote_numeric_string():
    assert quote_numeric_string(9860) == '"9860"'
    assert quote_numeric_string("9860") == '"9860"'
    assert quote_numeric_string('"9860"') == '"9860"'
    assert quote_numeric_string("nine86zero") == "nine86zero"
    assert quote_numeric_string("9860zero") == "9860zero"
    assert quote_numeric_string("98.60") == '"98.60"'


def test_quote_numeric_string_query_strings():
    assert "code={}".format(quote_numeric_string("9860")) == 'code="9860"'
    assert "code={}".format(quote_numeric_string('"9860"')) == 'code="9860"'
    assert "code={}".format(quote_numeric_string("nine86zero")) == "code=nine86zero"
    assert "code={}".format(quote_numeric_string("98.60")) == 'code="98.60"'


def test_get_sanitized_filename_preserve_alpha():
    assert (
        get_sanitized_filename("abcdefghijklmnopqrstuvwxyz")
        == "abcdefghijklmnopqrstuvwxyz"
    )
    assert (
        get_sanitized_filename("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    )


def test_get_sanitized_filename_preserve_numeric():
    assert get_sanitized_filename("01234567890") == "01234567890"


def test_get_sanitized_filename_preserve_special():
    assert get_sanitized_filename("._-") == "._-"


def test_get_sanitized_filename_remove_special():
    assert get_sanitized_filename("_a*b:c<d>e%f/(g)h+i_0.txt") == "_abcde%f(g)h+i_0.txt"
    assert get_sanitized_filename('fi:l*e/p"a?t>h|.t<xt') == "filepath.txt"


def test_get_sanitized_filename_t2_star():
    assert get_sanitized_filename("t2*.dicom.zip") == "t2star.dicom.zip"
    assert get_sanitized_filename("t2 *.dicom.zip") == "t2 star.dicom.zip"
    assert get_sanitized_filename("t2_*.dicom.zip") == "t2_star.dicom.zip"


def test_hash_value():
    assert (
        hash_value("0123456789")
        == "84d89877f0d4041efb6bf91a16f0248f2fd573e6af05c19f96bedb9f882f7882"
    )
    assert (
        hash_value("0123456789", output_format="dec")
        == "13221615211924021243025110724926222403614347213115230175519315915019021915913647120130"
    )
    assert (
        hash_value("0123456789", salt="0123456789")
        == "4e76ad8354461437c04ef9b9b242540b6406d782ff2c3fb28afdab5b423f88fe"
    )
    assert callable(hash_value("0123456789", output_format="junk"))


def test_get_common_list_dict():
    dict_1 = {"InstanceNumber": 1, "PatientID": "Flywheel", "SeriesNumber": 1}
    dict_2 = {"InstanceNumber": 2, "PatientID": "Flywheel", "SeriesNumber": 1}
    dict_3 = {"InstanceNumber": 2, "PatientID": "Flywheel", "SeriesNumber": 1}
    dict_list = [dict_1, dict_2, dict_3]
    expected_dict = {"PatientID": "Flywheel", "SeriesNumber": 1}
    assert get_dict_list_common_dict(dict_list) == expected_dict
    assert get_dict_list_common_dict([]) == dict()


def test_false_if_exc_is_timeout():
    assert false_if_exc_is_timeout(TypeError())
    assert all(
        [false_if_exc_is_timeout(ApiException(status=x)) for x in [404, 403, 400]]
    )
    assert not any(
        [false_if_exc_is_timeout(ApiException(status=x)) for x in [500, 502, 504]]
    )


def test_false_if_exc_is_timeout_or_sub_exists():
    # test that false_if_exc_is_timeout behavior preserved
    assert false_if_exc_is_timeout_or_sub_exists(TypeError())
    assert all(
        [
            false_if_exc_is_timeout_or_sub_exists(ApiException(status=x))
            for x in [404, 403, 400]
        ]
    )
    assert not any(
        [
            false_if_exc_is_timeout_or_sub_exists(ApiException(status=x))
            for x in [500, 502, 504]
        ]
    )

    def set_detail(exc, detail_str):
        setattr(exc, "detail", detail_str)
        return exc

    has_sub_detail = 'subject code "flywheel" already exists in project project_id'
    assert not any(
        [
            false_if_exc_is_timeout_or_sub_exists(
                set_detail(ApiException(status=x), has_sub_detail)
            )
            for x in [409, 422]
        ]
    )
    assert all(
        [
            false_if_exc_is_timeout_or_sub_exists(ApiException(status=x))
            for x in [409, 422]
        ]
    )
