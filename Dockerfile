FROM python:3.7-slim-stretch
MAINTAINER Flywheel <support@flywheel.io>

COPY requirements.txt /opt

RUN pip install -r /opt/requirements.txt

WORKDIR /flywheel/v0

COPY manifest.json \
     run.py \
     util.py \
     dicom_metadata.py \
     Dockerfile \
     ./

RUN chmod +x run.py