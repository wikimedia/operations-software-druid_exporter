# Copyright 2017 Luca Toscano
#                Filippo Giunchedi
#                Wikimedia Foundation
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from collections import defaultdict
from druid_exporter.collector import DruidCollector
from druid_exporter.exporter import check_metrics_config_file_consistency, parse_metrics_config_file


class TestDruidCollector(unittest.TestCase):

    def setUp(self):
        # List of metric names as emitted by Druid, coupled with their
        # Prometheus metric name and labels.
        metrics_config = {
            'broker': {
                "query/time": {
                    "prometheus_metric_name": "druid_broker_query_time_ms",
                    "type": "histogram",
                    "buckets": ["10", "100", "500", "1000", "2000", "3000", "5000", "7000", "10000", "inf", "sum"],
                    "labels": ["dataSource"],
                    "description": "Milliseconds taken to complete a query."
                },
                "query/bytes": {
                    "druid_metric_name": "query/bytes",
                    "prometheus_metric_name": "druid_broker_query_bytes",
                    "buckets": ["10", "100", "500", "1000", "2000", "3000", "5000", "7000", "10000", "inf", "sum"],
                    "type": "histogram",
                    "labels": ["dataSource"],
                    "description": "Number of bytes returned in query response."
                },
                "query/cache/total/numEntries": {
                    "prometheus_metric_name": "druid_broker_query_cache_numentries_count",
                    "type": "gauge",
                    "labels": [],
                    "description": "Number of cache entries."
                },
            },
            'historical': {
                "query/time": {
                    "prometheus_metric_name": "druid_historical_query_time_ms",
                    "type": "histogram",
                    "buckets": ["10", "100", "500", "1000", "2000", "3000", "5000", "7000", "10000", "inf", "sum"],
                    "labels": ["dataSource"],
                    "description": "Milliseconds taken to complete a query."
                },
                "query/bytes": {
                    "druid_metric_name": "query/bytes",
                    "prometheus_metric_name": "druid_historical_query_bytes",
                    "buckets": ["10", "100", "500", "1000", "2000", "3000", "5000", "7000", "10000", "inf", "sum"],
                    "type": "histogram",
                    "labels": ["dataSource"],
                    "description": "Number of bytes returned in query response."
                },
                "query/cache/total/numEntries": {
                    "prometheus_metric_name": "druid_broker_historical_cache_numentries_count",
                    "type": "gauge",
                    "labels": [],
                    "description": "Number of cache entries."
                },
                "segment/used": {
                    "prometheus_metric_name": "druid_historical_segment_used",
                    "type": "gauge",
                    "labels": ["tier", "dataSource"],
                    "description": "Segments used."
                },
                "segment/count": {
                    "prometheus_metric_name": "druid_historical_segment_count",
                    "type": "gauge",
                    "labels": ["tier", "dataSource"],
                    "description": "Segments count."
                },
                "query/cache/total/evictions": {
                    "prometheus_metric_name": "druid_historical_query_cache_evictions_total",
                    "type": "gauge",
                    "labels": ["dataSource"],
                    "description": "Cache evictions total.ƒ"
                },
            },
            'coordinator': {
                "segment/assigned/count": {
                    "prometheus_metric_name": "druid_coordinator_segment_assigned_count",
                    "type": "gauge",
                    "labels": ["tier"],
                    "description": "Segments assigned."
                },
                "segment/underReplicated/count": {
                    "prometheus_metric_name": "druid_coordinator_segment_under_replicated_count",
                    "type": "gauge",
                    "labels": ["tier", "dataSource"],
                    "description": "Segments underreplicated."
                },
                "segment/overShadowed/count": {
                    "prometheus_metric_name": "druid_coordinator_segment_overshadowed_count",
                    "type": "gauge",
                    "labels": [],
                    "description": "Segments over shadowed ."
                },
                "segment/count": {
                    "prometheus_metric_name": "druid_coordinator_segment_count",
                    "type": "gauge",
                    "labels": ["dataSource"],
                    "description": "Segments count."
                },
            },
        }
        self.collector = DruidCollector(metrics_config)

    def test_check_metrics_config_file_consistency(self):
        """Test if the config file checker raises the appropriate validation errors.
        """
        wrong_config = {}
        with self.assertRaises(RuntimeError):
            check_metrics_config_file_consistency(wrong_config)
        wrong_config = {'not-a-druid-daemon': {'test/metric': 'test'}}
        with self.assertRaises(RuntimeError):
            check_metrics_config_file_consistency(wrong_config)
        wrong_config = {
            'broker': {
                "segment/count": {
                    # Missing field "prometheus_metric_name": "druid_coordinator_segment_count",
                    "type": "gauge",
                    "labels": ["dataSource"],
                    "description": "Segments count."
                }
            }
        }
        with self.assertRaises(RuntimeError):
            check_metrics_config_file_consistency(wrong_config)
        wrong_config = {
            'broker': {
                "segment/count": {
                    "prometheus_metric_name": "druid_coordinator_segment_count",
                    # Missing field "type": "gauge",
                    "labels": ["dataSource"],
                    "description": "Segments count."
                }
            }
        }
        with self.assertRaises(RuntimeError):
            check_metrics_config_file_consistency(wrong_config)
        wrong_config = {
            'broker': {
                "segment/count": {
                    "prometheus_metric_name": "druid_coordinator_segment_count",
                    "type": "gauge",
                    # Missing field "labels": ["dataSource"],
                    "description": "Segments count."
                 }
            }
        }
        with self.assertRaises(RuntimeError):
            check_metrics_config_file_consistency(wrong_config)
        wrong_config = {
            'broker': {
                "segment/count": {
                    "prometheus_metric_name": "druid_coordinator_segment_count",
                    "type": "gauge",
                    "labels": ["dataSource"],
                    # Missing field "description": "Segments count."
                }
            }
        }
        with self.assertRaises(RuntimeError):
            check_metrics_config_file_consistency(wrong_config)
        wrong_config = {
            'broker': {
                "query/bytes": {
                    "druid_metric_name": "query/bytes",
                    "prometheus_metric_name": "druid_broker_query_bytes",
                    "type": "histogram",
                    "labels": ["dataSource"],
                    "description": "Number of bytes returned in query response."
                    # no buckets field
                }
            }
        }
        with self.assertRaises(RuntimeError):
            check_metrics_config_file_consistency(wrong_config)

    def test_store_histogram(self):
        """Check that multiple datapoints modify the self.histograms data-structure
           in the expected way.
        """
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'query/time', 'value': 42}
        self.collector.register_datapoint(datapoint)
        expected_struct = {
            'query/time': {
                'historical': {
                    tuple(['test']): {
                        '10': 0, '100': 1, '500': 1, '1000': 1, '2000': 1, '3000': 1,
                        '5000': 1, '7000': 1, '10000': 1, 'inf': 1, 'sum': 42.0}}}}
        expected_result = defaultdict(lambda: {}, expected_struct)
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'query/time', 'value': 5}
        self.collector.register_datapoint(datapoint)
        for bucket in expected_struct['query/time']['historical'][('test',)]:
            if bucket != 'sum':
                expected_struct['query/time']['historical'][('test',)][bucket] += 1
            else:
                expected_struct['query/time']['historical'][('test',)][bucket] += 5
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test2',
                     'metric': 'query/time', 'value': 5}
        self.collector.register_datapoint(datapoint)
        expected_result['query/time']['historical'][('test2',)] = {
            '10': 1, '100': 1, '500': 1, '1000': 1, '2000': 1, '3000': 1, '5000': 1, '7000': 1,
            '10000': 1, 'inf': 1, 'sum': 5.0}
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/broker', 'dataSource': 'test',
                     'metric': 'query/time', 'value': 42}
        self.collector.register_datapoint(datapoint)
        expected_result['query/time']['broker'] = {
                ('test',): {'10': 0, '100': 1, '500': 1, '1000': 1, '2000': 1, '3000': 1,
                            '5000': 1, '7000': 1, '10000': 1, 'inf': 1, 'sum': 42.0}}
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/broker', 'dataSource': 'test',
                     'metric': 'query/time', 'value': 600}
        self.collector.register_datapoint(datapoint)
        for bucket in expected_struct['query/time']['broker'][('test',)]:
            if bucket == 'sum':
                expected_struct['query/time']['broker'][('test',)][bucket] += 600
            elif 600 <= float(bucket):
                expected_struct['query/time']['broker'][('test',)][bucket] += 1
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/broker', 'dataSource': 'test2',
                     'metric': 'query/time', 'value': 5}
        self.collector.register_datapoint(datapoint)
        expected_result['query/time']['broker'][('test2',)] = {
            '10': 1, '100': 1, '500': 1, '1000': 1, '2000': 1, '3000': 1, '5000': 1, '7000': 1,
            '10000': 1, 'inf': 1, 'sum': 5.0}
        self.assertEqual(self.collector.histograms, expected_result)

    def test_store_counter(self):
        """Check that multiple datapoints modify the self.counters data-structure
           in the expected way.
        """
        # First datapoint should add the missing layout to the data structure
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'segment/used', 'tier': '_default_tier', 'value': 42}
        self.collector.register_datapoint(datapoint)
        expected_struct = {'segment/used': {'historical': {('_default_tier', 'test'): 42.0}}}
        expected_result = defaultdict(lambda: {}, expected_struct)
        self.assertEqual(self.collector.counters, expected_result)

        # Second datapoint for the same daemon but different metric should create
        # the missing layout without touching the rest.
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'query/cache/total/evictions', 'value': 142}
        self.collector.register_datapoint(datapoint)
        expected_result['query/cache/total/evictions'] = {'historical': {('test',): 142.0}}
        self.assertEqual(self.collector.counters, expected_result)

        # Third datapoint for the same metric as used in the first test, should
        # add a key to the already existent dictionary.
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test2',
                     'metric': 'segment/count', 'tier': '_default_tier', 'value': 543}
        self.collector.register_datapoint(datapoint)
        expected_result['segment/count'] = {'historical': {('_default_tier', 'test2'): 543.0}}
        self.assertEqual(self.collector.counters, expected_result)

        # Fourth datapoint for an already seen metric but different daemon
        datapoint = {'feed': 'metrics', 'service': 'druid/coordinator', 'dataSource': 'test',
                     'metric': 'segment/count', 'value': 111}
        self.collector.register_datapoint(datapoint)
        expected_result['segment/count']['coordinator'] = {('test',): 111.0}
        self.assertEqual(self.collector.counters, expected_result)

        # Fifth datapoint should override a pre-existent value
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'segment/used', 'tier': '_default_tier', 'value': 11}
        self.collector.register_datapoint(datapoint)
        expected_result['segment/used']['historical'][('_default_tier', 'test')] = 11.0
        self.assertEqual(self.collector.counters, expected_result)

    def test_store_datapoint_not_supported_in_config(self):
        """Check if a datapoint not supported by the config is correctly handled.
        """
        datapoint = {
            "feed": "metrics", "timestamp": "2017-11-14T13:08:20.819Z",
            "service": "druid/historical", "host": "druid1001.eqiad.wmnet:8083",
            "metric": "segment/scan/pending", "value": 0}
        datapoints_registered = self.collector.datapoints_registered
        self.collector.register_datapoint(datapoint)
        self.assertEqual(self.collector.datapoints_registered, datapoints_registered)

    def test_datapoint_without_configured_label(self):
        """Test the use case of a datapoint related to a supported metric not
           carrying a value related to a configured label (admin misconfiguration,
           change in Druid, etc..).
        """
        # Missing label "dataSource"
        datapoint = {
            'feed': 'metrics', 'service': 'druid/historical',
            'metric': 'query/time', 'value': 42}
        self.collector.register_datapoint(datapoint)

        # Missing label "tier"
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'segment/used', 'value': 42}
        self.collector.register_datapoint(datapoint)

    def test_add_one_datapoint_for_each_metric(self):
        """Add one datapoint for each metric and make sure that they render correctly
           when running collect()
        """
        datapoints = [
            {"feed": "metrics",
             "timestamp": "2017-11-14T16:25:01.395Z",
             "service": "druid/broker",
             "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/time",
             "value": 10,
             "context": "{\"queryId\":\"b09649a1-a440-463f-8b7e-6b476cc22d45\",\"timeout\":40000}",
             "dataSource": "NavigationTiming",
             "duration": "PT94670899200S", "hasFilters": "false",
             "id": "b09649a1-a440-463f-8b7e-6b476cc22d45",
             "interval": ["0000-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"],
             "remoteAddress": "10.64.53.26", "success": "true",
             "type": "timeBoundary", "version": "0.9.2"},

            {"feed": "metrics",
             "timestamp": "2017-11-14T16:25:01.395Z",
             "service": "druid/historical",
             "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/time",
             "value": 1,
             "context": "{\"queryId\":\"b09649a1-a440-463f-8b7e-6b476cc22d45\",\"timeout\":40000}",
             "dataSource": "NavigationTiming",
             "duration": "PT94670899200S", "hasFilters": "false",
             "id": "b09649a1-a440-463f-8b7e-6b476cc22d45",
             "interval": ["0000-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z"],
             "remoteAddress": "10.64.53.26", "success": "true",
             "type": "timeBoundary", "version": "0.9.2"},

            {"feed": "metrics", "timestamp": "2017-11-14T13:11:55.581Z",
             "service": "druid/broker", "host": "druid1001.eqiad.wmnet:8083",
             "metric": "query/bytes", "value": 1015,
             "context": "{\"bySegment\":true,\"finalize\":false,\"populateCache\":false,\
                          \"priority\": 0,\"queryId\":\"d96c4b73-8e9b-4a43-821d-f194b4e134d7\",\
                          \"timeout\":40000}",
             "dataSource": "webrequest", "duration": "PT3600S",
             "hasFilters": "false", "id": "d96c4b73-8e9b-4a43-821d-f194b4e134d7",
             "interval": ["2017-11-14T11:00:00.000Z/2017-11-14T12:00:00.000Z"],
             "remoteAddress": "10.64.5.101", "type": "segmentMetadata",
             "version": "0.9.2"},

            {"feed": "metrics", "timestamp": "2017-11-14T13:11:55.581Z",
             "service": "druid/historical", "host": "druid1001.eqiad.wmnet:8083",
             "metric": "query/bytes", "value": 1015,
             "context": "{\"bySegment\":true,\"finalize\":false,\"populateCache\":false,\
                         \"priority\": 0,\"queryId\":\"d96c4b73-8e9b-4a43-821d-f194b4e134d7\"\
                         ,\"timeout\":40000}",
             "dataSource": "webrequest", "duration": "PT3600S", "hasFilters": "false",
             "id": "d96c4b73-8e9b-4a43-821d-f194b4e134d7",
             "interval": ["2017-11-14T11:00:00.000Z/2017-11-14T12:00:00.000Z"],
             "remoteAddress": "10.64.5.101", "type": "segmentMetadata",
             "version": "0.9.2"},

            {"feed": "metrics", "timestamp": "2017-11-14T16:25:39.217Z",
             "service": "druid/broker", "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/cache/total/numEntries", "value": 5350},

            {"feed": "metrics", "timestamp": "2017-11-14T16:25:39.217Z",
             "service": "druid/historical", "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/cache/total/numEntries", "value": 5351},

            {"feed": "metrics", "timestamp": "2017-11-14T13:08:20.820Z",
             "service": "druid/historical", "host": "druid1001.eqiad.wmnet:8083",
             "metric": "query/cache/total/evictions", "dataSource": "test", "value": 0},

            {"feed": "metrics", "timestamp": "2017-11-14T13:07:20.823Z",
             "service": "druid/historical", "host": "druid1001.eqiad.wmnet:8083",
             "metric": "segment/count", "value": 41, "dataSource": "netflow",
             "priority": "0", "tier": "_default_tier"},

            {"feed": "metrics", "timestamp": "2017-11-14T12:14:53.697Z",
             "service": "druid/coordinator", "host": "druid1001.eqiad.wmnet: 8081",
             "metric": "segment/count", "value": 56, "dataSource": "netflow"},

            {"feed": "metrics", "timestamp": "2017-12-07T09:55:04.937Z",
             "service": "druid/historical", "host": "druid1001.eqiad.wmnet:8083",
             "metric": "segment/used", "value": 3252671142,
             "dataSource": "banner_activity_minutely",
             "priority": "0", "tier": "_default_tier"},

            {"feed": "metrics", "timestamp": "2017-11-14T16:15:15.577Z",
             "service": "druid/coordinator",
             "host": "druid1001.eqiad.wmnet:8081", "metric": "segment/assigned/count",
             "value": 0.0, "tier": "_default_tier"},

            {"feed": "metrics",
             "timestamp": "2017-11-14T16:19:46.564Z",
             "service": "druid/coordinator", "host": "druid1001.eqiad.wmnet:8081",
             "metric": "segment/overShadowed/count", "value": 0.0},

            {"feed": "metrics",
             "timestamp": "2017-11-14T16:27:48.310Z",
             "service": "druid/coordinator",
             "host": "druid1001.eqiad.wmnet:8081",
             "metric": "segment/underReplicated/count", "value": 0,
             "dataSource": "unique_devices_per_project_family_monthly",
             "tier": "_default_tier"}
        ]

        # The following datapoint registration batch should not generate
        # any exception (breaking the test).
        for datapoint in datapoints:
            self.collector.register_datapoint(datapoint)

        # Running it twice should not produce more metrics
        for datapoint in datapoints:
            self.collector.register_datapoint(datapoint)

        collected_metrics = 0
        prometheus_metric_samples = []
        for metric in self.collector.collect():
            # Metrics should not be returned if no sample is associated
            # (not even a 'nan')
            self.assertNotEqual(metric.samples, [])
            if metric.samples and metric.samples[0][0].startswith('druid_'):
                collected_metrics += 1
                prometheus_metric_samples.append(metric.samples)

        # Number of metrics pushed using register_datapoint plus the ones
        # generated by the exporter for bookeeping,
        # like druid_exporter_datapoints_registered_total
        expected_druid_metrics_len = len(datapoints) + 1
        self.assertEqual(collected_metrics, expected_druid_metrics_len)

        for datapoint in datapoints:
            metric = datapoint['metric']
            daemon = datapoint['service'].split('/')[1]
            prometheus_metric_name = self.collector.metrics_config[daemon][metric]['prometheus_metric_name']
            prometheus_metric_labels = self.collector.metrics_config[daemon][metric]['labels']

            # The prometheus metric samples are in two forms:
            # 1) histograms:
            # [('druid_broker_query_time_ms_bucket', {'datasource': 'NavigationTiming', 'le': '10'}, 2),
            #  [...]
            #  ('druid_broker_query_time_ms_bucket', {'datasource': 'NavigationTiming', 'le': 'inf'}, 2),
            #  ('druid_broker_query_time_ms_count', {'datasource': 'NavigationTiming'}, 2),
            #  ('druid_broker_query_time_ms_sum', {'datasource': 'NavigationTiming'}, 20.0)]
            #
            # 2) counter/gauge
            #    [('druid_coordinator_segment_unneeded_count', {'tier': '_default_tier'}, 0.0)]
            #
            # The idea of the following test is to make sure that after sending
            # one data point for each metric, the sample contains the information
            # needed.
            #
            if 'query' not in metric:
                for sample in prometheus_metric_samples:
                    if prometheus_metric_name == sample[0][0]:
                        if prometheus_metric_labels:
                            for label in prometheus_metric_labels:
                                self.assertTrue(label.lower() in sample[0][1])
                        else:
                            self.assertTrue(sample[0][1] == {})
                        break
            else:
                for sample in [s for s in prometheus_metric_samples if len(s) == 8]:
                    if metric in sample[0][0]:
                        bucket_counter = 0
                        sum_counter = 0
                        count_counter = 0
                        for s in sample:
                            if s[0] == metric + "_sum":
                                sum_counter += 1
                            elif s[0] == metric + "_count":
                                count_counter += 1
                            elif s[0] == metric + "_bucket":
                                bucket_counter += 1
                            else:
                                raise RuntimeError(
                                    'Histogram sample not supported: {}'
                                    .format(s))
                        assertEqual(sum_counter, 1)
                        assertEqual(count_counter, 1)
                        assertEqual(bucket_counter, 6)
                        break
                else:
                    RuntimeError(
                        'The metric {} does not have a valid sample!'
                        .format(metric))

    def test_register_datapoints_count(self):
        datapoints = [

            {"feed": "metrics", "timestamp": "2017-11-14T16:25:39.217Z",
             "service": "druid/broker", "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/cache/total/numEntries", "value": 1},

            {"feed": "metrics", "timestamp": "2017-11-14T17:25:39.217Z",
             "service": "druid/broker", "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/cache/total/numEntries", "value": 2},

            {"feed": "metrics", "timestamp": "2017-11-14T18:25:39.217Z",
             "service": "druid/broker", "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/cache/total/numEntries", "value": 3},
        ]

        for datapoint in datapoints:
            self.collector.register_datapoint(datapoint)

        self.assertEqual(self.collector.datapoints_registered, 3)
