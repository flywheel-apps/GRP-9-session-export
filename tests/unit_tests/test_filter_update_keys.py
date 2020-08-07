import pandas as pd
from dicom_metadata import filter_update_keys, get_dicom_df


def test_skip_df_logic_for_list_of_1():
    update_keys = ['PatientID', 'SeriesDescription', 'StudyDescription']
    result = filter_update_keys(update_keys, ['does_not_exist.dcm'])
    assert result == update_keys


def test_return_df_if_dict_list_empty():
    result = get_dicom_df([], [])
    assert isinstance(result, pd.DataFrame)
