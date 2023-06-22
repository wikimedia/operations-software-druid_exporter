ARG BASE_IMAGE=python:3.9-slim-bullseye
FROM $BASE_IMAGE

COPY . .
RUN python setup.py install

EXPOSE 8000

ENTRYPOINT ["druid_exporter"]