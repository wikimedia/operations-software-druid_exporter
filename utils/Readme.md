# Utilities to help with the generation of new config files

## Description

Since the amount of metrics that druid can generate is big and can change from version to version we developed this small utility to convert the error messages from the exporter into configuration blocks that can be used with the new json configuration.

The idea is to run the exporter in debug mode `-d` and send the output to the `parse_failed_logs.py` script.

The script will look for lines with the format:


    DEBUG:druid_exporter.collector:The following datapoint is not supported, either because the 'feed' field is not 'metrics' or the metric itself is not supported: {'feed': 'metrics', 'timestamp': '2020-05-07T09:57:08.983Z', 'service': 'druid/router', 'host': '10.132.0.27:8888', 'version': '0.18.0', 'metric': 'query/time', 'value': 7, 'context': {'queryId': '2c08a196-9c11-49c3-a5e7-b41357388f24', 'timeout': 40000}, 'dataSource': 'datasource_test_1', 'duration': 'PT9223372036854775.807S', 'hasFilters': 'false', 'id': '2c08a196-9c11-49c3-a5e7-b41357388f24', 'interval': ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'], 'remoteAddress': '10.0.3.217', 'success': 'true', 'type': 'timeBoundary'}

and it will output something like this:

    ----------------
    Found metric: 
    {
        "context": {
            "queryId": "2c08a196-9c11-49c3-a5e7-b41357388f24", 
            "timeout": 40000
        }, 
        "dataSource": "datasource_test_1", 
        "duration": "PT9223372036854775.807S", 
        "feed": "metrics", 
        "hasFilters": "false", 
        "host": "10.132.0.27:8888", 
        "id": "2c08a196-9c11-49c3-a5e7-b41357388f24", 
        "interval": [
            "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
        ], 
        "metric": "query/time", 
        "remoteAddress": "10.0.3.217", 
        "service": "druid/router", 
        "success": "true", 
        "timestamp": "2020-05-07T09:57:08.983Z", 
        "type": "timeBoundary", 
        "value": 7, 
        "version": "0.18.0"
    }

    This is a metric for: druid/router
    Adding dataSource as label for prometheus.
    {
        "query/time": {
            "buckets": [
                "10", 
                "100", 
                "500", 
                "1000", 
                "2000", 
                "3000", 
                "5000", 
                "7000", 
                "10000", 
                "inf", 
                "sum"
            ], 
            "description": "Milliseconds taken to complete a query.", 
            "labels": [
                "dataSource"
            ], 
            "prometheus_metric_name": "druid_query_time", 
            "type": "histogram"
        }
    }

## Notes

The file with the descriptions is generated from the druid official page. <https://druid.apache.org/docs/latest/operations/metrics.html>

If you want to send the output via pipe you need to redirect the output like this:

    druid_exporter -d ../conf/druid_0_18_0.json 2>&1 | ./parse_failed_logs.py

Every missing metric is parsed only once per execution. So you won't see the same block several times.

I can't guarantee that the heuristic to guess if the metric should be gauge or histogram or the labels is perfect.

Keep in mind that this is just an utility to help you to find and generate new configuration blocks for missing metrics, but you should review them before adding them to the final config file.
