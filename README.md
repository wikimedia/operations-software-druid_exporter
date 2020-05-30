# Prometheus exporter for Druid (http://druid.io/)

Collects HTTP POST JSON data coming from Druid daemons and expose them formatted
following the [Prometheus](https://prometheus.io) standards.

## Running

The easiest way to run `druid_exporter` is via a virtualenv:

```
  virtualenv .venv
  .venv/bin/python setup.py install
  .venv/bin/druid_exporter path-to-metrics-config-file.json
```

By default metrics are exposed on TCP port `8000`. Python 2 is not supported.

### Running with Docker

Issue the following command to build the image. Default base image (python:3.8-alpine) can be replaced.
```bash
docker build -t druid-exporter:latest .
docker build -t druid-exporter:latest --build-arg BASE_IMAGE=<python-image> .
```

Issue the following command to run.
```bash
docker run -it -d -p 8000:8000 druid-exporter:latest conf/example_druid_0_12_3.json
```

Example `docker-compose.yml`:
```yaml
druid-exporter:
  image: "druid-exporter:latest"
  command: 
    - "conf/example_druid_0_12_3.json"
  ports:
    - published: 8000
      target: 8000
```

## Druid versions supported

This exporter is tested and used by the Wikimedia foundation with Druid version 0.12.3,
so it might not work as expected for newer versions. The exporter is designed in such a way
that metrics are configured entirely by admin/users, so it should be resilient to future
versions of Druid.

## How does it work?

Druid can be configured to emit metrics (JSON data) to a HTTP endpoint configured
via the following runtime properties:

http://druid.io/docs/0.12.3/configuration/index.html#http-emitter-module

The druid prometheus exporter accepts HTTP POST data, inspects it and stores/aggregates
every supported datapoint into a data structure. It then formats the
data on the fly to Prometheus metrics when a GET /metrics is requested.

The exporter is also able to pull metrics from a Kafka topic, when Druid is configured to do so:

https://druid.apache.org/docs/latest/development/extensions-contrib/kafka-emitter.html

The Kafka support is optional and should be used when the number of metrics/datapoints emitted
by Druid is very high (rule of thumb could be around 1000 datapoints/s). Please check the section
about performance considerations below for more info.

This exporter is supposed to be run on each host running one or multiple Druid daemons.

## Supported metrics and labels

In theory any metric that Druid emits should be supported by this exporter, even if there might
be some use cases not covered (if it happens to you, please report the problem!).
As example, let's start with the following metric emitted by druid:

```
[{"feed":"metrics","timestamp":"2020-04-23T11:36:27.866Z","service":"druid/peon","host":"druid1001.eqiad.wmnet:8201",
"version":"0.12.3","metric":"ingest/events/unparseable","value":0,"dataSource":"wmf_netflow",
"taskId":["index_kafka_wmf_netflow_ee74c5a5820837c_hedlopdm"]}]
```

The exporter doesn't know anything about what metrics it should support, everything is stated
in the metrics JSON config file that must be provided as parameter. In this case, we could add the following
entry to the JSON to support the metric:

```
[..]
    "peon": {
    	[..]
        "ingest/events/unparseable": {
            "prometheus_metric_name": "druid_realtime_ingest_events_unparseable_count",
            "type": "gauge",
            "labels": ["dataSource"],
            "description": "Number of events rejected because the events are unparseable."
        },
        [..]
[..]
```

Pleae note that the name of the label corresponds to the field in the "feed" event emitted
by Druid.
If you want to check a more complete example, please see the conf directory of this repository.

Please check the following document for more info about metrics emitted by Druid:
http://druid.io/docs/0.12.3/operations/metrics.html

The JVM metrics are currently not supported, please check other projects
like https://github.com/prometheus/jmx_exporter if you need to collect them.

## Known limitations

When a Druid cluster is running with multiple coordinators or overlords,
only one of them acts as leader and the others are in standby mode. From the metrics
emission point of view, this means that only the daemon acting as leader emits druid metrics,
the others don't (except for metrics like Jetty ones). This has caused some confusion
in our day to day operations, since when a coordinator or a overlord leader looses
its status in favor of another one (for example due to a restart) it stops emitting
metrics, and the Prometheus exporter keeps reporting the last known state. This might
be confusing to see at first (expecially if metrics are aggregated) so the current
"fix" is to restart the Druid Prometheus exporter when a coordinator or a overlord
leader are restarted. Future versions of this project might contain a fix, pull
requests are welcome!

## Performance considerations

In https://github.com/wikimedia/operations-software-druid_exporter/issues/11 some users
brought up an interesting use case for the exporter, namely handling peaks of 1500 datapoints/s
sent from Druid brokers (one can check the rate of datapoints/s via the
`druid_exporter_datapoints_registered_total metric`).

The exporter's code was refactored to be able to scale more
(if you are curious, check [this commit](https://github.com/wikimedia/operations-software-druid_exporter/commit/f22c6d9f8707ae2d274db9b10669b971beed64ab)), but it wasn't enough, since users kept reporting timeouts from Druid daemons while sending
datapoints to the exporter. There are some recommendations to follow:
* Try to use a dedicated exporter instance for daemons sending high volumes of datapoints.
* Try to tune the HTTP emitter's settings via https://druid.apache.org/docs/latest/configuration/index.html#http-emitter-module
  (if the Druid version that you are running supports them).
If the above is not enough, the exporter can be configured to also pull datapoints from
a Kafka topic (see [https://druid.apache.org/docs/latest/development/extensions-contrib/kafka-emitter.html](Kafka)). With this configuration, the exporter will ingest datapoints coming via
HTTP and Kafka at the same time. An ideal solution is to force Druid daemons emitting too many
datapoints/s to use the KafkaEmitter, and the other ones to use the HTTPEmitter.