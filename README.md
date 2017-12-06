# Prometheus exporter for Druid (http://druid.io/)

Collects HTTP POST JSON data coming from Druid daemons and expose them formatted
following the [Prometheus](https://prometheus.io) standards.

## Running

The easiest way to run `druid_exporter` is via a virtualenv:

  virtualenv .venv
  .venv/bin/python setup.py
  .venv/bin/druid_exporter

By default metrics are exposed on TCP port `8000`. Python 2 is not supported.

## Druid versions supported

This exporter is tested and used by the Wikimedia foundation with Druid version 0.9.2,
so it might not work as expected for newer versions.

## How does it work?

Druid can be configured to emit metrics (JSON data) to a HTTP endpoint configured
via the following runtime properties:

http://druid.io/docs/0.9.2/configuration/index.html#http-emitter-module

The druid prometheus exporter accepts HTTP POST data, inspects it and stores/aggregates
every supported datapoint into a data structure. It then formats the
data on the fly to Prometheus metrics when a GET /metrics is requested.

This exporter is supposed to be run on each host running a Druid daemon.

## Supported metrics and labels

### Broker, Historical (histograms)
* `query/time` [datasource]
* `query/bytes` [datasource]
* `query/cache/total/numEntries`
* `query/cache/total/sizeBytes`
* `query/cache/total/hits`
* `query/cache/total/misses`
* `query/cache/total/evictions`
* `query/cache/total/timeouts`
* `query/cache/total/errors`

### Historical, Coordinator (counters)
* `segment/max` [datasource]
* `segment/count` [datasource]

### Historical (counters)
* `segment/used` [datasource]
* `segment/scan/pending`

### Coordinator (counters)
* `segment/assigned/count` [tier]
* `segment/moved/count` [tier]
* `segment/dropped/count` [tier]
* `segment/deleted/count` [tier]
* `segment/unneeded/count` [tier]
* `segment/overShadowed/count`
* `segment/loadQueue/failed` [server]
* `segment/loadQueue/count` [server]
* `segment/dropQueue/count` [server]
* `segment/size` [datasource]
* `segment/unavailable/count` [datasource]
* `segment/underReplicated/count` [datasource, tier]

### Peon (counters)
* `query/time` [datasource]
* `query/bytes` [datasource]
* `ingest/events/thrownAway` [dataSource]
* `ingest/events/unparseable` [dataSource]
* `ingest/events/processed` [dataSource]
* `ingest/rows/output` [dataSource]
* `ingest/persists/count` [dataSource]
* `ingest/persists/failed` [dataSource]
* `ingest/handoff/failed` [dataSource]
* `ingest/handoff/count` [dataSource]

Realtime metrics have been tested only when emitted by Peons, since the Wikimedia
use case (for the moment) is to use [Tranquillity](https://github.com/druid-io/tranquility)
rather than Real Time nodes.

Please check the following document for more info:
http://druid.io/docs/0.9.2/operations/metrics.html

The JVM metrics are currently not supported, please check other projects
like https://github.com/prometheus/jmx_exporter if you need to collect them.
