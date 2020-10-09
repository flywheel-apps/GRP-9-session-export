from unittest.mock import patch, MagicMock
import pytest
import logging
import json

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
        if string == 'MR':
            return mr_modality
        else:
            return {'id': string, 'classification': None}


class GearContext:
    def __init__(self):
        self.client = Client()
        self.config_json = {"yml-functional_basename_regex": "^oink$"}
        self.destination = {"id": "dontneednostinkingdestination"}


def test_get_file_classification():
    fw = GearContext().client
    classification = {'Measurement': ['B0']}
    test_file = flywheel.FileEntry(classification=classification, modality="MR")
    result = run.get_file_classification(fw, test_file, "MR", "test_name")
    assert result == classification
    test_file = flywheel.FileEntry(modality="MR")
    result = run.get_file_classification(fw, test_file, "MR", "test_name")
    assert result is None
    classification = {'Measurement': ['Bananas']}
    test_file = flywheel.FileEntry(classification=classification)
    result = run.get_file_classification(fw, test_file, "MR", "test_name")
    assert result is None


def test_get_file_modality():
    test_file = flywheel.FileEntry(modality="MR")
    result = run.get_file_modality(test_file, "test.dcm")
    assert result == "MR"
    test_file = flywheel.FileEntry()
    result = run.get_file_modality(test_file, "test.dcm")
    assert result is None
    result = run.get_file_modality(test_file, "test.dcm.mriqc.qa.html")
    assert result == "MR"


def test_format_file_metadata_upload_str():
    fw = GearContext().client
    test_file = flywheel.FileEntry()
    setattr(test_file, "info", dict())
    result = run.format_file_metadata_upload_str(fw, test_file, "test.txt", {})
    assert result == "{}"
    test_metadata = {"modality": "MR", "classification": {'Measurement': ['B0']}, "type": "dicom", "info": {"spam": "eggs"}}
    test_file = flywheel.FileEntry(**test_metadata)
    result = run.format_file_metadata_upload_str(fw, test_file, "test.txt", {})
    assert result == json.dumps(test_metadata)
