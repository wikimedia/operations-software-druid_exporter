ARG BASE_IMAGE=python:3.8-alpine
FROM $BASE_IMAGE

COPY conf/ /exporter/conf/
COPY druid_exporter/ /exporter/druid_exporter/
COPY setup.py /exporter/setup.py

WORKDIR /exporter

RUN apk add build-base
RUN python ./setup.py install

EXPOSE 8000

ENTRYPOINT ["python", "-u", "druid_exporter/exporter.py"]
