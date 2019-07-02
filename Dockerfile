# FROM python:3-alpine
FROM flywheel/dicom-metadata-import-grp-3:1.0.1
MAINTAINER Flywheel <support@flywheel.io>

RUN pip install \
      flywheel-sdk \
      pydicom \
      pytz \
      tzlocal \
      jsonschema

WORKDIR /flywheel/v0

COPY manifest.json \
     run \
     dicom_metadata.py \
     Dockerfile \
     ./
RUN chmod +x run dicom_metadata.py


ENTRYPOINT ["/flywheel/v0/run"]
