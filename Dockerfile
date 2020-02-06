# FROM python:3-alpine
FROM python:3.7
MAINTAINER Flywheel <support@flywheel.io>

RUN pip install \
      pip==19.3.1 \
      flywheel-sdk==10.7.1 \
      pydicom==1.4.1 \
      pytz==2019.3 \
      tzlocal==2.0.0 \
      jsonschema==3.1.1 \
      nibabel \
      pandas \
      numpy

WORKDIR /flywheel/v0

COPY manifest.json \
     run \
     util.py \
     dicom_metadata.py \
     Dockerfile \
     ./
RUN chmod +x run dicom_metadata.py util.py


ENTRYPOINT ["/flywheel/v0/run"]
