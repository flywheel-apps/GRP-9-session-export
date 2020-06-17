import flywheel
from run import get_patientsex_from_subject


def test_invalid_sex():
    subject = flywheel.Subject(sex='invalid')
    result = get_patientsex_from_subject(subject)
    assert result == ''
    subject = flywheel.Subject()
    result = get_patientsex_from_subject(subject)
    assert result == ''


def test_valid_sex():
    subject = flywheel.Subject(sex='male')
    result = get_patientsex_from_subject(subject)
    assert result == 'M'

    subject = flywheel.Subject(sex='female')
    result = get_patientsex_from_subject(subject)
    assert result == 'F'

    subject = flywheel.Subject(sex='other')
    result = get_patientsex_from_subject(subject)
    assert result == 'O'
