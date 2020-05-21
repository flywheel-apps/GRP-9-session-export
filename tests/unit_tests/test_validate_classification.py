
from unittest.mock import patch, MagicMock
import pytest
import logging


import flywheel

import run

mr_modality = {'classification': {'Features': ['Quantitative',
                                               'Multi-Shell',
                                               'Multi-Echo',
                                               'Multi-Flip',
                                               'Multi-Band',
                                               'Steady-State',
                                               '3D',
                                               'Compressed-Sensing',
                                               'Eddy-Current-Corrected',
                                               'Fieldmap-Corrected',
                                               'Gradient-Unwarped',
                                               'Motion-Corrected',
                                               'Physio-Corrected',
                                               'Derived',
                                               'In-Plane',
                                               'Phase',
                                               'Magnitude',
                                               '2D',
                                               'AAscout',
                                               'Spin-Echo',
                                               'Gradient-Echo',
                                               'EPI',
                                               'WASSR',
                                               'FAIR',
                                               'FAIREST',
                                               'PASL',
                                               'EPISTAR',
                                               'PICORE',
                                               'pCASL',
                                               'MPRAGE',
                                               'MP2RAGE',
                                               'FLAIR',
                                               'SWI',
                                               'QSM',
                                               'RMS',
                                               'DTI',
                                               'DSI',
                                               'DKI',
                                               'HARDI',
                                               'NODDI',
                                               'Water-Reference',
                                               'Transmit-Reference',
                                               'SBRef',
                                               'Uniform',
                                               'Singlerep',
                                               'QC',
                                               'TRACE',
                                               'FA',
                                               'MIP',
                                               'Navigator',
                                               'Contrast-Agent',
                                               'Phase-Contrast',
                                               'TOF',
                                               'VASO',
                                               'iVASO',
                                               'DSC',
                                               'DCE',
                                               'Task',
                                               'Resting-State',
                                               'PRESS',
                                               'STEAM',
                                               'M0',
                                               'Phase-Reversed',
                                               'Spiral',
                                               'SPGR',
                                               'Control',
                                               'Label'],
                                  'Intent': ['Localizer',
                                             'Shim',
                                             'Calibration',
                                             'Fieldmap',
                                             'Structural',
                                             'Functional',
                                             'Screenshot',
                                             'Non-Image',
                                             'Spectroscopy'],
                                  'Measurement': ['B0',
                                                  'B1',
                                                  'T1',
                                                  'T2',
                                                  'T2*',
                                                  'PD',
                                                  'MT',
                                                  'Perfusion',
                                                  'Diffusion',
                                                  'Susceptibility',
                                                  'Fingerprinting',
                                                  'MRA',
                                                  'CEST',
                                                  'T1rho',
                                                  'SVS',
                                                  'CSI',
                                                  'EPSI',
                                                  'BOLD',
                                                  'Phoenix']},
               'id': 'MR'}


class Client:
    def get_modality(self, string):
        return mr_modality

class GearContext:
    def __init__(self):
        self.client = Client()
        self.config_json = {"yml-functional_basename_regex": "^oink$"}
        self.destination = {"id": "dontneednostinkingdestination"}


def test_validate_classification_basic_function_works(caplog):

    caplog.set_level(logging.DEBUG)

    fw = GearContext().client

    classification = {'Measurement': ['B0']}
    return_value = run._validate_classification(fw, 'MR', classification, 'filename')
    assert return_value == True

    classification = {'Measurement': ['FLAIR']}
    return_value = run._validate_classification(fw, 'MR', classification, 'filename')
    assert return_value == False
    assert 'filename, modality "MR", "FLAIR" is not valid for "Measurement"' in caplog.messages[0]


def test_validate_classification_multiple_works():

    fw = GearContext().client

    classification = {'Features': ['MPRAGE', '3D']}
    return_value = run._validate_classification(fw, 'MR', classification, 'filename')
    assert return_value == True

    classification = {'Features': ['Derived', 'fail']}
    return_value = run._validate_classification(fw, 'MR', classification, 'filename')
    assert return_value == False


def test_validate_classification_bad_class_key_fail(caplog):

    caplog.set_level(logging.DEBUG)

    fw = GearContext().client

    classification = {'NotToBeFound': ['FLAIR']}
    return_value = run._validate_classification(fw, 'MR', classification, 'filename')
    assert return_value == False
    assert 'modality "MR", "NotToBeFound" is not in classification schema' in caplog.messages[0]


def test_validate_classification_bad_modality_fail(caplog):

    caplog.set_level(logging.DEBUG)

    fw = GearContext().client

    with patch.object(fw,'get_modality') as mock:
        mock.side_effect = flywheel.ApiException("oof ApiException", "doc", None)

        classification = {'Features': ['MPRAGE', '3D']}
        return_value = run._validate_classification(fw, 'CT', classification, 'filename')

        assert return_value == False
        assert "oof ApiException" in caplog.messages[0]


    # _export_files(fw, acquisition, export_acquisition, session, subject, project, config):
