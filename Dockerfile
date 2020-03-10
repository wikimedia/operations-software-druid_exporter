FROM python:3.7.6-slim-stretch

WORKDIR /app

COPY setup.py /app
COPY druid_exporter/ /app/druid_exporter

RUN python -m pip install virtualenv && \
    python -m virtualenv .venv && \
    .venv/bin/python setup.py install

ENTRYPOINT [".venv/bin/druid_exporter", "-d"]

EXPOSE 8000
