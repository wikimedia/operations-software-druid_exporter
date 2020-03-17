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
import json
import unittest

from collections import defaultdict
from druid_exporter.collector import DruidCollector


class TestDruidCollector(unittest.TestCase):

    def setUp(self):
        self.collector = DruidCollector()

        # List of metric names as emitted by Druid, coupled with their
        # Prometheus metric name and labels.
        self.supported_metric_names = {
            'broker': {
                'query/time': {
                    'metric_name': 'druid_broker_query_time_ms',
                    'labels': ['dataSource']
                },
                'query/bytes': {
                    'metric_name': 'druid_broker_query_bytes',
                    'labels': ['dataSource']
                },
                'query/cache/total/numEntries': {
                    'metric_name': 'druid_broker_query_cache_numentries_count',
                    'labels': None
                },
                'query/cache/total/sizeBytes': {
                    'metric_name': 'druid_broker_query_cache_size_bytes',
                    'labels': None
                },
                'query/cache/total/hits': {
                    'metric_name': 'druid_broker_query_cache_hits_count',
                    'labels': None
                },
                'query/cache/total/misses': {
                    'metric_name':'druid_broker_query_cache_hits_count',
                    'labels': None
                },
                'query/cache/total/evictions': {
                    'metric_name':'druid_broker_query_cache_evictions_count',
                    'labels': None
                },
                'query/cache/total/timeouts': {
                    'metric_name':'druid_broker_query_cache_timeouts_count',
                    'labels': None
                },
                'query/cache/total/errors': {
                    'metric_name':'druid_broker_query_cache_errors_count',
                    'labels': None
                },
                'query/count': {
                    'metric_name': 'druid_broker_query_count',
                    'labels': None,
                },
                'query/success/count': {
                    'metric_name': 'druid_broker_query_success_count',
                    'labels': None,
                },
                'query/failed/count': {
                    'metric_name': 'druid_broker_query_failed_count',
                    'labels': None,
                },
                'query/interrupted/count': {
                    'metric_name': 'druid_broker_query_interrupted_count',
                    'labels': None,
                },
            },
            'historical': {
                'query/time': {
                    'metric_name': 'druid_historical_query_time_ms',
                    'labels': ['dataSource']
                },
                'query/bytes': {
                    'metric_name': 'druid_historical_query_bytes',
                    'labels': ['dataSource']
                },
                'query/cache/total/numEntries': {
                    'metric_name': 'druid_historical_query_cache_numentries_count',
                    'labels': None
                },
                'query/cache/total/sizeBytes': {
                    'metric_name': 'druid_historical_query_cache_size_bytes',
                    'labels': None
                },
                'query/cache/total/hits': {
                    'metric_name': 'druid_historical_query_cache_hits_count',
                    'labels': None
                },
                'query/cache/total/misses': {
                    'metric_name':'druid_historical_query_cache_hits_count',
                    'labels': None
                },
                'query/cache/total/evictions': {
                    'metric_name':'druid_historical_query_cache_evictions_count',
                    'labels': None
                },
                'query/cache/total/timeouts': {
                    'metric_name':'druid_historical_query_cache_timeouts_count',
                    'labels': None
                },
                'query/cache/total/errors': {
                    'metric_name':'druid_historical_query_cache_errors_count',
                    'labels': None
                },
                'segment/count': {
                    'metric_name': 'druid_historical_segment_count',
                    'labels': ['tier', 'dataSource']
                },
                'segment/max': {
                    'metric_name': 'druid_historical_max_segment_bytes',
                    'labels': None
                },
                'segment/used': {
                    'metric_name': 'druid_historical_segment_used_bytes',
                    'labels': ['tier', 'dataSource']
                },
                'segment/scan/pending': {
                    'metric_name': 'druid_historical_segment_scan_pending',
                    'labels': None
                },
                'query/count': {
                    'metric_name': 'druid_historical_query_count',
                    'labels': None,
                },
                'query/success/count': {
                    'metric_name': 'druid_historical_query_success_count',
                    'labels': None,
                },
                'query/failed/count': {
                    'metric_name': 'druid_historical_query_failed_count',
                    'labels': None,
                },
                'query/interrupted/count': {
                    'metric_name': 'druid_historical_query_interrupted_count',
                    'labels': None,
                },
            },
            'coordinator': {
                'segment/assigned/count': {
                    'metric_name': 'druid_coordinator_segment_assigned_count',
                    'labels': ['tier'],
                },
                'segment/moved/count': {
                    'metric_name': 'druid_coordinator_segment_moved_count',
                    'labels': ['tier']
                },
                'segment/dropped/count': {
                    'metric_name': 'druid_coordinator_segment_dropped_count',
                    'labels': ['tier']
                },
                'segment/deleted/count': {
                    'metric_name': 'druid_coordinator_segment_deleted_count',
                    'labels': ['tier']
                },
                'segment/unneeded/count': {
                    'metric_name': 'druid_coordinator_segment_unneeded_count',
                    'labels': ['tier']
                },
                'segment/overShadowed/count': {
                    'metric_name': 'druid_coordinator_segment_overshadowed_count',
                    'labels': None
                },
                'segment/loadQueue/failed': {
                    'metric_name': 'druid_coordinator_segment_loadqueue_failed_count',
                    'labels': ['server']
                },
                'segment/loadQueue/count': {
                    'metric_name': 'druid_coordinator_segment_loadqueue_count',
                    'labels': ['server']
                },
                'segment/dropQueue/count': {
                    'metric_name': 'druid_coordinator_segment_dropqueue_count',
                    'labels': ['server']
                },
                'segment/size': {
                    'metric_name': 'druid_coordinator_segment_size_bytes',
                    'labels': ['dataSource']
                },
                'segment/count': {
                    'metric_name': 'druid_coordinator_segment_count',
                    'labels': ['dataSource']
                },
                'segment/unavailable/count': {
                    'metric_name': 'druid_coordinator_segment_unavailable_count',
                    'labels': ['dataSource']
                },
                'segment/underReplicated/count': {
                    'metric_name': 'druid_coordinator_segment_under_replicated_count',
                    'labels': ['tier', 'dataSource']
                }
            },
            'peon': {
                'query/time': {
                    'metric_name': 'druid_peon_query_time_ms',
                    'labels': ['dataSource']
                },
                'query/bytes': {
                    'metric_name': 'druid_peon_query_bytes',
                    'labels': ['dataSource']
                },
                'ingest/events/thrownAway': {
                    'metric_name': 'druid_realtime_ingest_events_thrown_away_count',
                    'labels': ['dataSource'],
                },
                'ingest/events/unparseable': {
                    'metric_name': 'druid_realtime_ingest_events_unparseable_count',
                    'labels': ['dataSource'],
                },
                'ingest/events/processed': {
                    'metric_name': 'druid_realtime_ingest_events_processed_count',
                    'labels': ['dataSource'],
                },
                'ingest/rows/output': {
                    'metric_name': 'druid_realtime_ingest_rows_output_count',
                    'labels': ['dataSource'],
                },
                'ingest/persists/count': {
                    'metric_name': 'druid_realtime_ingest_persists_count',
                    'labels': ['dataSource'],
                },
                'ingest/persists/failed': {
                    'metric_name': 'druid_realtime_ingest_persists_failed_count',
                    'labels': ['dataSource'],
                },
                'ingest/handoff/failed': {
                    'metric_name': 'druid_realtime_ingest_handoff_failed_count',
                    'labels': ['dataSource'],
                },
                'ingest/handoff/count': {
                    'metric_name': 'druid_realtime_ingest_handoff_count',
                    'labels': ['dataSource'],
                }
            },
            'overlord': {
                'ingest/kafka/lag': {
                    'metric_name': 'druid_overlord_ingest_kafka_lag',
                    'labels': ['dataSource'],
                },
                'task/run/time': {
                    'metric_name': 'druid_overlord_task_run_time_ms',
                    'labels': ['dataSource', 'taskStatus', 'taskType', 'host'],
                },
                'segment/added/bytes': {
                    'metric_name': 'druid_overlord_segment_added_bytes_count',
                    'labels': ['dataSource', 'interval', 'taskId', 'taskType'],
                },
                'segment/moved/bytes': {
                    'metric_name': 'druid_overlord_segment_moved_bytes_count',
                    'labels': ['dataSource', 'interval', 'taskId', 'taskType'],
                },
                'segment/nuked/bytes': {
                    'metric_name': 'druid_overlord_segment_nuked_bytes_count',
                    'labels': ['dataSource', 'interval', 'taskId', 'taskType'],
                },
                'jetty/numOpenConnections': {
                    'metric_name': 'druid_overlord_jetty_num_open_conenctions',
                    'labels': None
                }
            },
            'middleManager': {
                'query/time': {
                    'metric_name': 'druid_middlemanager_query_time_ms',
                    'labels': ['dataSource']
                },
                'query/bytes': {
                    'metric_name': 'druid_middlemanager_query_bytes',
                    'labels': ['dataSource']
                },
                'ingest/events/thrownAway': {
                    'metric_name': 'druid_middlemanager_ingest_events_thrown_away_count',
                    'labels': ['dataSource'],
                },
                'ingest/events/unparseable': {
                    'metric_name': 'druid_middlemanager_ingest_events_unparseable_count',
                    'labels': ['dataSource'],
                },
                'ingest/events/processed': {
                    'metric_name': 'druid_middlemanager_ingest_events_processed_count',
                    'labels': ['dataSource'],
                },
                'ingest/rows/output': {
                    'metric_name': 'druid_middlemanager_ingest_rows_output_count',
                    'labels': ['dataSource'],
                },
                'ingest/persists/count': {
                    'metric_name': 'druid_middlemanager_ingest_persists_count',
                    'labels': ['dataSource'],
                },
                'ingest/persists/failed': {
                    'metric_name': 'druid_middlemanager_ingest_persists_failed_count',
                    'labels': ['dataSource'],
                },
                'ingest/handoff/failed': {
                    'metric_name': 'druid_middlemanager_ingest_handoff_failed_count',
                    'labels': ['dataSource'],
                },
                'ingest/events/messageGap': {
                    'metric_name': 'druid_middlemanager_ingest_events_message_gap',
                    'labels': ['dataSource'],
                },
                'ingest/handoff/count': {
                    'metric_name': 'druid_middlemanager_ingest_handoff_count',
                    'labels': ['dataSource'],
                }
            }
        }
        self.metrics_without_labels = [
            'druid_historical_segment_scan_pending',
            'druid_historical_max_segment_bytes',
            'druid_coordinator_segment_overshadowed_count',
            'druid_broker_query_cache_numentries_count',
            'druid_broker_query_cache_size_bytes',
            'druid_broker_query_cache_hits_count',
            'druid_broker_query_cache_misses_count',
            'druid_broker_query_cache_evictions_count',
            'druid_broker_query_cache_timeouts_count',
            'druid_broker_query_cache_errors_count',
            'druid_broker_success_query_count',
            'druid_broker_interrupted_query_count',
            'druid_broker_failed_query_count',
            'druid_broker_query_count',
            'druid_historical_query_cache_numentries_count',
            'druid_historical_query_cache_size_bytes',
            'druid_historical_query_cache_hits_count',
            'druid_historical_query_cache_misses_count',
            'druid_historical_query_cache_evictions_count',
            'druid_historical_query_cache_timeouts_count',
            'druid_historical_query_cache_errors_count',
            'druid_historical_success_query_count',
            'druid_historical_interrupted_query_count',
            'druid_historical_failed_query_count',
            'druid_historical_query_count',
            'druid_exporter_datapoints_registered_total',
            'druid_overlord_jetty_num_open_connections'
        ]

    def test_store_histogram(self):
        """Check that multiple datapoints modify the self.histograms data-structure
           in the expected way.
        """
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'query/time', 'value': 42}
        self.collector.register_datapoint(datapoint)
        expected_struct = {
            'query/time': {
                'historical':
                    {'test': {'10': 0, '100': 1, '500': 1, '1000': 1, '2000': 1, '3000': 1, '5000': 1, '7000': 1, '10000': 1, 'inf': 1, 'sum': 42.0}}}}
        expected_result = defaultdict(lambda: {}, expected_struct)
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'query/time', 'value': 5}
        self.collector.register_datapoint(datapoint)
        for bucket in expected_struct['query/time']['historical']['test']:
            if bucket != 'sum':
                expected_struct['query/time']['historical']['test'][bucket] += 1
            else:
                expected_struct['query/time']['historical']['test'][bucket] += 5
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test2',
                     'metric': 'query/time', 'value': 5}
        self.collector.register_datapoint(datapoint)
        expected_result['query/time']['historical']['test2'] = {'10': 1, '100': 1, '500': 1, '1000': 1,
                                                                '2000': 1, '3000': 1, '5000': 1, '7000': 1,
                                                                '10000': 1, 'inf': 1, 'sum': 5.0}
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/broker', 'dataSource': 'test',
                     'metric': 'query/time', 'value': 42}
        self.collector.register_datapoint(datapoint)
        expected_result['query/time']['broker'] = {
            'test': {'10': 0, '100': 1, '500': 1, '1000': 1, '2000': 1, '3000': 1, '5000': 1, '7000': 1, '10000': 1, 'inf': 1, 'sum': 42.0}}
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/broker', 'dataSource': 'test',
                     'metric': 'query/time', 'value': 600}
        self.collector.register_datapoint(datapoint)
        for bucket in expected_struct['query/time']['broker']['test']:
            if bucket == 'sum':
                expected_struct['query/time']['broker']['test'][bucket] += 600
            elif 600 <= float(bucket):
                expected_struct['query/time']['broker']['test'][bucket] += 1
        self.assertEqual(self.collector.histograms, expected_result)

        datapoint = {'feed': 'metrics', 'service': 'druid/broker', 'dataSource': 'test2',
                     'metric': 'query/time', 'value': 5}
        self.collector.register_datapoint(datapoint)
        expected_result['query/time']['broker']['test2'] = {'10': 1, '100': 1, '500': 1, '1000': 1,
                                                            '2000': 1, '3000': 1, '5000': 1, '7000': 1,
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
        expected_struct = yield {'segment/used': {'historical': {'_default_tier': {'test': 42.0}}}}
        expected_result = defaultdict(lambda: {}, expected_struct)
        self.assertEqual(self.collector.counters, expected_result)

        # Second datapoint for the same daemon but different metric should create
        # the missing layout without touching the rest.
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'query/cache/total/evictions', 'value': 142}
        self.collector.register_datapoint(datapoint)
        expected_result['query/cache/total/evictions'] = {'historical': 142.0}
        self.assertEqual(self.collector.counters, expected_result)

        # Third datapoint for the same metric as used in the first test, should
        # add a key to the already existent dictionary.
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test2',
                     'metric': 'segment/count', 'tier': '_default_tier', 'value': 543}
        self.collector.register_datapoint(datapoint)
        expected_result['segment/count'] = {'historical': {'_default_tier': {'test2': 543.0}}}
        self.assertEqual(self.collector.counters, expected_result)

        # Fourth datapoint for an already seen metric but different daemon
        datapoint = {'feed': 'metrics', 'service': 'druid/coordinator', 'dataSource': 'test',
                     'metric': 'segment/count', 'value': 111}
        self.collector.register_datapoint(datapoint)
        expected_result['segment/count']['coordinator'] = {'test': 111.0}
        self.assertEqual(self.collector.counters, expected_result)

        # Fifth datapoint should override a pre-existent value
        datapoint = {'feed': 'metrics', 'service': 'druid/historical', 'dataSource': 'test',
                     'metric': 'segment/used', 'tier': '_default_tier', 'value': 11}
        self.collector.register_datapoint(datapoint)
        expected_result['segment/used']['historical']['_default_tier']['test'] = 11.0
        self.assertEqual(self.collector.counters, expected_result)

    def test_metrics_without_datapoints(self):
        """Whenever a Prometheus metric needs to be rendered, it may happen that
           no datapoints have been registered yet. In case that the metric do not
           have any label associated with it, 'nan' will be returned, otherwise
           the metric will not be rendered.
        """
        druid_metric_names = []
        for metric in self.collector.collect():
            if not metric.samples[0][0].startswith("druid_") or \
                    "scrape" in metric.samples[0][0]:
                continue
            self.assertEqual(len(metric.samples), 1)
            druid_metric_names.append(metric.samples[0][0])

        self.assertEqual(set(druid_metric_names), set(self.metrics_without_labels))

    def test_add_one_datapoint_for_each_metric(self):
        """Add one datapoint for each metric and make sure that they render correctly
           when running collect()
        """
        with open('datapoints.json', 'r') as data_file:
            datapoints = json.load(data_file)

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
        # generated by the exporter for bookkeeping,
        # like druid_exporter_datapoints_registered_total
        expected_druid_metrics_len = len(datapoints) + 1
        self.assertEqual(collected_metrics, expected_druid_metrics_len)

        for datapoint in datapoints:
            metric = datapoint['metric']
            daemon = datapoint['service'].split('/')[1]
            prometheus_metric_name = self.supported_metric_names[daemon][metric]['metric_name']
            prometheus_metric_labels = self.supported_metric_names[daemon][metric]['labels']

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
                                self.assertTrue(label in sample[0][1],
                                                f'expected {label} label in {sample}')
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
             "metric": "query/cache/total/numEntries", "value": 5350},

            {"feed": "metrics", "timestamp": "2017-11-14T16:25:39.217Z",
             "service": "druid/broker", "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/cache/total/sizeBytes", "value": 23951931},

            {"feed": "metrics",
             "timestamp": "2017-11-14T16:25:39.217Z", "service": "druid/broker",
             "host": "druid1001.eqiad.wmnet:8082",
             "metric": "query/cache/total/hits", "value": 358547},
        ]

        for datapoint in datapoints:
            self.collector.register_datapoint(datapoint)

        self.assertEqual(self.collector.datapoints_registered, 3)
