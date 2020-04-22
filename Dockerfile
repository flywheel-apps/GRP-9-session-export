FROM python:3.7-slim-stretch
MAINTAINER Flywheel <support@flywheel.io>

COPY requirements.txt /tmp

RUN pip install -r /tmp/requirements.txt

WORKDIR /flywheel/v0

COPY manifest.json \
     run.py \
     util.py \
     dicom_metadata.py \
     Dockerfile \
     ./
RUN chmod +x run.py dicom_metadata.py util.py


ENTRYPOINT ["/flywheel/v0/run.py"]
