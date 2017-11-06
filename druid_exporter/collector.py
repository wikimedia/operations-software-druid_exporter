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

import logging

from collections import defaultdict
from prometheus_client.core import (CounterMetricFamily, GaugeMetricFamily,
                                    HistogramMetricFamily, Summary)


log = logging.getLogger(__name__)


class DruidCollector(object):
    scrape_duration = Summary(
            'druid_scrape_duration_seconds', 'Druid scrape duration')

    def __init__(self, allowed_daemons):
        # Datapoints successfully registered
        self.datapoints_registered = 0

        # List of supported metrics and their fields of the JSON dictionary
        # sent by a Druid daemon. These fields will be added as labels
        # when returning the available metrics in @collect.
        self.supported_metric_names = {
            # Broker, Historical
            'query/time': ['dataSource'],
            'query/bytes': ['dataSource'],
            'query/node/time': ['dataSource', 'server'],
            'query/node/bytes': ['dataSource', 'server'],
            'query/cache/total/numEntries': None,
            'query/cache/total/sizeBytes': None,
            'query/cache/total/hits': None,
            'query/cache/total/misses': None,
            'query/cache/total/evictions': None,
            'query/cache/total/timeouts': None,
            'query/cache/total/errors': None,
            # Historical + Coordinator
            'segment/count': ['dataSource'],
            # Historical
            'segment/max': None,
            'segment/used': ['tier', 'dataSource'],
            'segment/scan/pending': None,
            # Coordinator
            'segment/assigned/count': ['tier'],
            'segment/moved/count': ['tier'],
            'segment/dropped/count': ['tier'],
            'segment/deleted/count': ['tier'],
            'segment/unneeded/count': ['tier'],
            'segment/overShadowed/count': None,
            'segment/loadQueue/failed': ['server'],
            'segment/loadQueue/count': ['server'],
            'segment/dropQueue/count': ['server'],
            'segment/size': ['dataSource'],
            'segment/unavailable/count': ['dataSource'],
            'segment/underReplicated/count': ['tier', 'dataSource'],
        }

        # Buckets used when storing histogram metrics.
        # 'sum' is a special bucket that will be used to collect the sum
        # of all values ending up in the various buckets.
        self.metric_buckets = {
            'query/time': ['10', '100', '500', '1000', '10000', 'inf', 'sum'],
            'query/bytes': ['10', '100', '500', '1000', '10000', 'inf', 'sum'],
            'query/node/time': ['10', '100', '500', '1000', '10000', 'inf', 'sum'],
            'query/node/bytes': ['10', '100', '500', '1000', '10000', 'inf', 'sum'],
        }

        # Data structure holding histogram data
        # Format: {daemon: {metric_name: {bucket2: value, bucket2: value, ...}}
        self.histograms = defaultdict(lambda: {})
        self.histograms_metrics = set([
            'query/time',
            'query/bytes',
            'query/node/time',
            'query/node/bytes',
        ])

        # Data structure holding counters data
        # Format: {daemon: {label_name: {label2_name: value}}
        # The order of the labels listed in supported_metric_names is important
        # since it is reflected in this data structure. The layering is not
        # strictly important for the final prometheus metrics but it is simplifies
        # the code that creates them (collect method).
        self.counters = defaultdict(lambda: {})
        self.counters_metrics = set([
            'query/cache/total/numEntries',
            'query/cache/total/sizeBytes',
            'query/cache/total/hits',
            'query/cache/total/misses',
            'query/cache/total/evictions',
            'query/cache/total/timeouts',
            'query/cache/total/errors',
            'segment/max',
            'segment/count',
            'segment/used',
            'segment/scan/pending',
            'segment/assigned/count',
            'segment/moved/count',
            'segment/dropped/count',
            'segment/deleted/count',
            'segment/unneeded/count',
            'segment/overShadowed/count',
            'segment/loadQueue/failed',
            'segment/loadQueue/count',
            'segment/dropQueue/count',
            'segment/size',
            'segment/unavailable/count',
            'segment/underReplicated/count',
        ])

        self.allowed_daemons = allowed_daemons

    @staticmethod
    def sanitize_field(datapoint_field):
        return datapoint_field.replace('druid/', '').lower()

    def _get_query_histograms(self, daemon):
        return {
           'query/time': HistogramMetricFamily(
               'druid_' + daemon + '_query_time_ms',
               'Milliseconds taken to complete a query.',
               labels=['datasource']),
           'query/bytes': HistogramMetricFamily(
               'druid_' + daemon + '_query_bytes',
               'Number of bytes returned in query response.',
               labels=['datasource']),
           'query/node/time': HistogramMetricFamily(
               'druid_' + daemon + '_query_node_time_ms',
               'Milliseconds taken to query individual '
               'historical/realtime nodes.',
               labels=['datasource', 'server']),
           'query/node/bytes': HistogramMetricFamily(
               'druid_' + daemon + '_query_node_bytes',
               'number of bytes returned from querying individual '
               'historical/realtime nodes.',
               labels=['datasource', 'server']),
            }

    def _get_cache_counters(self, daemon):
        return {
            'query/cache/total/numEntries': GaugeMetricFamily(
               'druid_' + daemon + '_query_cache_numentries_count',
               'Number of cache entries.'),
            'query/cache/total/sizeBytes': GaugeMetricFamily(
               'druid_' + daemon + '_query_cache_sizebytes_count',
               'Size in bytes of cache entries.'),
            'query/cache/total/hits': GaugeMetricFamily(
               'druid_' + daemon + '_query_cache_hits_count',
               'Number of cache hits.'),
            'query/cache/total/misses': GaugeMetricFamily(
               'druid_' + daemon + '_query_cache_misses_count',
               'Number of cache misses.'),
            'query/cache/total/evictions': GaugeMetricFamily(
               'druid_' + daemon + '_query_cache_evictions_count',
               'Number of cache evictions.'),
            'query/cache/total/timeouts': GaugeMetricFamily(
               'druid_' + daemon + '_query_cache_timeouts_count',
               'Number of cache timeouts.'),
            'query/cache/total/errors': GaugeMetricFamily(
               'druid_' + daemon + '_query_cache_errors_count',
               'Number of cache errors.'),
            }

    def _get_historical_counters(self):
        return {
            'segment/max': GaugeMetricFamily(
               'druid_historical_max_segment_bytes',
               'Maximum byte limit available for segments.'),
            'segment/count': GaugeMetricFamily(
               'druid_historical_segment_count',
               'Number of served segments.',
               labels=['datasource']),
            'segment/used': GaugeMetricFamily(
               'druid_historical_segment_used_bytes',
               'Bytes used for served segments.',
               labels=['datasource']),
            'segment/scan/pending': GaugeMetricFamily(
               'druid_historical_segment_scan_pending',
               'Number of segments in queue waiting to be scanned.'),
            }

    def _get_coordinator_counters(self):
        return {
            'segment/assigned/count': GaugeMetricFamily(
               'druid_coordinator_segment_assigned_count',
               'Number of segments assigned to be loaded in the cluster.',
               labels=['tier']),
            'segment/moved/count': GaugeMetricFamily(
               'druid_coordinator_segment_moved_count',
               'Number of segments assigned to be loaded in the cluster.',
               labels=['tier']),
            'segment/dropped/count': GaugeMetricFamily(
               'druid_coordinator_segment_dropped_count',
               'Number of segments dropped due to being overshadowed.',
               labels=['tier']),
            'segment/deleted/count': GaugeMetricFamily(
               'druid_coordinator_segment_deleted_count',
               'Number of segments dropped due to rules.',
               labels=['tier']),
            'segment/unneeded/count': GaugeMetricFamily(
               'druid_coordinator_segment_unneeded_count',
               'Number of segments dropped due to being marked as unused.',
               labels=['tier']),
            'segment/overShadowed/count': GaugeMetricFamily(
               'druid_coordinator_segment_overshadowed_count',
               'Number of overShadowed segments.'),
            'segment/loadQueue/failed': GaugeMetricFamily(
               'druid_coordinator_segment_loadqueue_failed_count',
               'Number of segments that failed to load.',
               labels=['server']),
            'segment/loadQueue/count': GaugeMetricFamily(
               'druid_coordinator_segment_loadqueue_count',
               'Number of segments to load.',
               labels=['server']),
            'segment/dropQueue/count': GaugeMetricFamily(
               'druid_coordinator_segment_dropqueue_count',
               'Number of segments to drop.',
               labels=['server']),
            'segment/size': GaugeMetricFamily(
               'druid_coordinator_segment_size_bytes',
               'Size in bytes of available segments.',
               labels=['datasource']),
            'segment/count': GaugeMetricFamily(
               'druid_coordinator_segment_count',
               'Number of served segments.',
               labels=['datasource']),
            'segment/unavailable/count': GaugeMetricFamily(
               'druid_coordinator_segment_unavailable_count',
               'Number of segments (not including replicas) left to load '
               'until segments that should be loaded in the cluster '
               'are available for queries.',
               labels=['datasource']),
            'segment/underReplicated/count': GaugeMetricFamily(
               'druid_coordinator_segment_under_replicated_count',
               'Number of segments (including replicas) left to load until '
               'segments that should be loaded in the cluster are '
               'available for queries.',
               labels=['tier', 'datasource']),
            }

    def store_counter(self, datapoint):
        """ This function adds data to the self.counters dictiorary following its
            convention, creating on the fly the missing bits. For example, given:
            self.counters = {}
            datapoint = {'service': 'druid/broker', 'metric'='segment/size',
                         'datasource': 'test', 'value': 10}

            This function will creates the following:
            self.counters = {'segment/size': {'broker': {'test': 10}}}

            The algorithm is generic enough to support all metrics handled by
            self.counters without caring about the number of labels needed.
        """
        daemon_name = DruidCollector.sanitize_field(str(datapoint['service']))
        metric_name = str(datapoint['metric'])
        metric_value = float(datapoint['value'])

        metrics_storage = self.counters[metric_name]
        metric_labels = self.supported_metric_names[metric_name]

        metrics_storage.setdefault(daemon_name, {})

        if metric_labels:
            metrics_storage_cursor = metrics_storage[daemon_name]
            for label in metric_labels:
                label_value = str(datapoint[label])
                if metric_labels[-1] != label:
                    metrics_storage_cursor.setdefault(label_value, {})
                    metrics_storage_cursor = metrics_storage_cursor[label_value]
                else:
                    metrics_storage_cursor[label_value] = metric_value
        else:
            metrics_storage[daemon_name] = metric_value

        log.debug("The datapoint {} modified the counters dictionary to: \n{}"
                  .format(datapoint, self.counters))

    def store_histogram(self, datapoint):
        """ Store datapoints that will end up in histogram buckets using a dictiorary.
            This function is highly customized for the only histograms configured
            so far, rather than being generic like store_counter. Example of how
            it works:
            self.histograms = {}
            datapoint = {'service': 'druid/broker', 'metric'='query/time',
                         'datasource': 'test', 'value': 10}

            This function will creates the following:
            self.counters = {'query/time': {'broker':
                {'test': {'10': 1, '100': 1, etc.., 'sum': 10}}}}}
        """
        daemon_name = DruidCollector.sanitize_field(str(datapoint['service']))
        metric_name = str(datapoint['metric'])
        metric_value = float(datapoint['value'])
        datasource = str(datapoint['dataSource'])

        self.histograms.setdefault(metric_name, {daemon_name: {datasource: {}}})
        self.histograms[metric_name].setdefault(daemon_name, {datasource: {}})
        self.histograms[metric_name][daemon_name].setdefault(datasource, {})

        for bucket in self.metric_buckets[metric_name]:
            stored_buckets = self.histograms[metric_name][daemon_name][datasource]
            if bucket not in stored_buckets:
                stored_buckets[bucket] = 0
            if bucket != 'sum' and metric_value <= float(bucket):
                    stored_buckets[bucket] += 1
        stored_buckets['sum'] += metric_value

        log.debug("The datapoint {} modified the histograms dictionary to: \n{}"
                  .format(datapoint, self.histograms))

    @scrape_duration.time()
    def collect(self):
        # Metrics common to Broker and Historical
        for daemon in ['broker', 'historical']:
            query_metrics = self._get_query_histograms(daemon)
            cache_metrics = self._get_cache_counters(daemon)

            for metric in query_metrics:
                if not self.histograms[metric]:
                    continue
                if daemon in self.histograms[metric]:
                    for datasource in self.histograms[metric][daemon]:
                        buckets = self.histograms[metric][daemon][datasource]
                        buckets_without_sum = [(k, v) for k, v in buckets.items() if k != 'sum']
                        query_metrics[metric].add_metric(
                            [datasource], buckets=buckets_without_sum,
                            sum_value=self.histograms[metric][daemon][datasource]['sum'])
                    yield query_metrics[metric]

            for metric in cache_metrics:
                if not self.counters[metric] or daemon not in self.counters[metric]:
                    if not self.supported_metric_names[metric]:
                        cache_metrics[metric].add_metric([], float('nan'))
                    else:
                        continue
                else:
                    cache_metrics[metric].add_metric([], self.counters[metric][daemon])
                yield cache_metrics[metric]

        historical_health_metrics = self._get_historical_counters()
        coordinator_metrics = self._get_coordinator_counters()
        for daemon, metrics in [('coordinator', coordinator_metrics),
                                ('historical', historical_health_metrics)]:
            for metric in metrics:
                if not self.counters[metric] or daemon not in self.counters[metric]:
                    if not self.supported_metric_names[metric]:
                        metrics[metric].add_metric([], float('nan'))
                    else:
                        continue
                else:
                    labels = self.supported_metric_names[metric]
                    if not labels:
                        metrics[metric].add_metric(
                            [], self.counters[metric][daemon])
                    elif len(labels) == 1:
                        for label in self.counters[metric][daemon]:
                            metrics[metric].add_metric(
                                [label], self.counters[metric][daemon][label])
                    else:
                        for outer_label in self.counters[metric][daemon]:
                            for inner_label in self.counters[metric][daemon][outer_label]:
                                metrics[metric].add_metric(
                                    [outer_label, inner_label],
                                    self.counters[metric][daemon][outer_label][inner_label])
                yield metrics[metric]

        registered = CounterMetricFamily('druid_exporter_datapoints_registered_count',
                                         'Number of datapoints successfully registered '
                                         'by the exporter.')
        registered.add_metric([], self.datapoints_registered)
        yield registered

    def register_datapoint(self, datapoint):
        if (datapoint['feed'] != 'metrics' or
                datapoint['metric'] not in self.supported_metric_names):
            log.debug("The following datapoint is not supported, either "
                      "because the 'feed' field is not 'metrics' or "
                      "the metric itself is not supported: {}"
                      .format(datapoint))
            return

        # Transform the metric name into a metrics' key
        daemon_name = DruidCollector.sanitize_field(str(datapoint['service']))

        if daemon_name not in self.allowed_daemons:
            log.error('Received metric from a daemon that is not allowed ({}), '
                      'dropping it.'.format(daemon_name))
            return

        metric_name = str(datapoint['metric'])
        if metric_name in self.histograms_metrics:
            self.store_histogram(datapoint)
        elif metric_name in self.counters_metrics:
            self.store_counter(datapoint)

        self.datapoints_registered += 1
