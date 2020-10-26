# import pytest
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
print(sys.path)
from export_report import DiffRecord, ExportComparison, setup
import flywheel


class TestSetup:
    def test_export_report(self):
        fw = flywheel.Client()
        analysis_job = fw.get_job("5f9705bcd4b2b317ce35f156")
        analysis = fw.get_analysis(analysis_job.destination.id)

        source, dest = setup(analysis.id, fw)

        comp = ExportComparison(source, dest, fw)
        comp.compare()
        comp.report()


x = TestSetup()
x.test_export_report()

