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

import re
import logging

from prometheus_client.core import (Counter, Gauge, Histogram, Summary)

log = logging.getLogger(__name__)

SKIP_METRIC = {
    'type': 'skip',
}

QUERY_TIME_SUMMARY_METRIC = {
    'labels': ['type', 'dataSource'],
    'suffix': '_ms',
    'type': 'summary',
}

QUERY_NODE_TIME_METRIC = {
    'labels': ['type', 'dataSource', 'server'],
    'suffix': '_ms',
    'type': 'summary',
}

SEGMENT_TIER_METRIC = {
    'labels': ['tier'],
}

SEGMENT_SOURCE_METRIC = {
    'labels': ['dataSource'],
}

SEGMENT_SERVER_METRIC = {
    'labels': ['server'],
}

SEGMENT_USED_METRIC = {
    'labels': ['dataSource', 'tier', 'priority'],
}

INGEST_METRIC = {
    'labels': ['dataSource'],
    'type': 'counter',
}

INGEST_TIME_METRIC = {
    'labels': ['dataSource'],
    'suffix': '_ms',
    'type': 'summary',
}

JETTY_METRICS = {
    'jetty/numOpenConnections': {},
}

QUERY_METRICS = {
    'query/bytes': {'labels': ['type', 'dataSource'], 'type': 'summary'},
    'query/cpu/time': {**QUERY_TIME_SUMMARY_METRIC},
    'query/time': {'labels': ['type', 'dataSource'], 'suffix': '_ms', 'type': 'histogram', 'buckets': (150, 1000, 5000, 10000, 30000, float('inf'))},
}

QUERY_CACHE_METRICS = {
    'query/cache/caffeine/delta/evictionBytes': {**SKIP_METRIC},
    'query/cache/caffeine/delta/loadTime': {**SKIP_METRIC},
    'query/cache/caffeine/delta/requests': {**SKIP_METRIC},
    'query/cache/caffeine/total/evictionBytes': {},
    'query/cache/caffeine/total/loadTime': {},
    'query/cache/caffeine/total/requests': {},
    'query/cache/delta/averageBytes': {**SKIP_METRIC},
    'query/cache/delta/errors': {**SKIP_METRIC},
    'query/cache/delta/evictions': {**SKIP_METRIC},
    'query/cache/delta/hitRate': {**SKIP_METRIC},
    'query/cache/delta/hits': {**SKIP_METRIC},
    'query/cache/delta/misses': {**SKIP_METRIC},
    'query/cache/delta/numEntries': {**SKIP_METRIC},
    'query/cache/delta/sizeBytes': {**SKIP_METRIC},
    'query/cache/delta/timeouts': {**SKIP_METRIC},
    'query/cache/total/averageBytes': {},
    'query/cache/total/errors': {},
    'query/cache/total/evictions': {},
    'query/cache/total/hitRate': {},
    'query/cache/total/hits': {},
    'query/cache/total/misses': {},
    'query/cache/total/numEntries': {},
    'query/cache/total/sizeBytes': {},
    'query/cache/total/timeouts': {},
}

QUERY_COUNT_METRIC = {
    'type': 'counter',
    'suffix': '',
}

QUERY_COUNT_STATS = {
    'query/failed/count': {**QUERY_COUNT_METRIC},
    'query/interrupted/count': {**QUERY_COUNT_METRIC},
    'query/success/count': {**QUERY_COUNT_METRIC},
}

SEGMENT_METRICS = {
    'query/segment/time': {**QUERY_TIME_SUMMARY_METRIC},
    'query/segmentAndCache/time': {**QUERY_TIME_SUMMARY_METRIC},
    'query/wait/time': {**QUERY_TIME_SUMMARY_METRIC},
    'segment/max': {'suffix': '_bytes'},
    'segment/scan/pending': {},
    'segment/used': {**SEGMENT_USED_METRIC, 'suffix': '_bytes'},
    'segment/usedPercent': {**SEGMENT_USED_METRIC, 'suffix': '_percent'},
    'segment/count': {**SEGMENT_USED_METRIC},
}

SEGMENT_COORDINATOR_METRICS = {
    'segment/assigned/count': {**SEGMENT_TIER_METRIC},
    'segment/deleted/count': {**SEGMENT_TIER_METRIC},
    'segment/dropped/count': {**SEGMENT_TIER_METRIC},
    'segment/moved/count': {**SEGMENT_TIER_METRIC},
    'segment/unneeded/count': {**SEGMENT_TIER_METRIC},

    'segment/cost/normalization': {**SEGMENT_TIER_METRIC, 'suffix': ''},
    'segment/cost/normalized': {**SEGMENT_TIER_METRIC, 'suffix': ''},
    'segment/cost/raw': {**SEGMENT_TIER_METRIC, 'suffix': ''},

    'segment/loadQueue/size': {**SEGMENT_SERVER_METRIC, 'suffix': '_bytes'},
    'segment/loadQueue/failed': {**SEGMENT_SERVER_METRIC},
    'segment/loadQueue/count': {**SEGMENT_SERVER_METRIC},
    'segment/dropQueue/count': {**SEGMENT_SERVER_METRIC},

    'segment/size': {**SEGMENT_SOURCE_METRIC, 'suffix': '_bytes'},
    'segment/count': {**SEGMENT_SOURCE_METRIC},
    'segment/unavailable/count': {**SEGMENT_SOURCE_METRIC},

    'segment/overShadowed/count': {},
    'segment/underReplicated/count': {'labels': ['tier', 'dataSource']},
}

BROKER_METRICS = {
    'avatica/remote/JsonHandler/Handler/Serialization': {**SKIP_METRIC},
    'avatica/server/AvaticaJsonHandler/Handler/RequestTimings': {**SKIP_METRIC},
    'query/intervalChunk/time': {**QUERY_TIME_SUMMARY_METRIC},
    'query/node/bytes': {'labels': ['type', 'dataSource', 'server'], 'type': 'summary'},
    'query/node/time': {**QUERY_NODE_TIME_METRIC},
    'query/node/ttfb': {**QUERY_NODE_TIME_METRIC},
}

INGEST_METRICS = {
    'ingest/events/messageGap': {'labels': ['dataSource'], 'suffix': '_ms'},
    'ingest/events/processed': {**INGEST_METRIC},
    'ingest/events/thrownAway': {**INGEST_METRIC},
    'ingest/events/unparseable': {**INGEST_METRIC},
    'ingest/handoff/count': {**INGEST_METRIC},
    'ingest/handoff/failed': {**INGEST_METRIC},
    'ingest/kafka/lag': {'labels': ['dataSource'], 'suffix': '_size'},
    'ingest/merge/cpu': {**SKIP_METRIC},
    'ingest/merge/time': {**INGEST_TIME_METRIC},
    'ingest/persists/backPressure': {**INGEST_TIME_METRIC},
    'ingest/persists/count': {**INGEST_METRIC},
    'ingest/persists/cpu': {**SKIP_METRIC},
    'ingest/persists/failed': {**INGEST_METRIC},
    'ingest/persists/time': {**INGEST_TIME_METRIC},
    'ingest/rows/output': {**INGEST_METRIC},
    'ingest/sink/count': {**INGEST_METRIC},
}

class DruidCollector(object):
    datapoints_processed = Counter('druid_exporter_datapoints_processed_count', '')

    def __init__(self):
        self.supported_metrics = {
            'coordinator': {
                **JETTY_METRICS,
                **SEGMENT_COORDINATOR_METRICS,
            },
            'broker': {
                **JETTY_METRICS,
                **QUERY_METRICS,
                **QUERY_CACHE_METRICS,
                **QUERY_COUNT_STATS,
                **BROKER_METRICS,
                **SEGMENT_METRICS,
            },
            'historical': {
                **JETTY_METRICS,
                **QUERY_METRICS,
                **QUERY_CACHE_METRICS,
                **QUERY_COUNT_STATS,
                **SEGMENT_METRICS,
            },
            'overlord': {
                **JETTY_METRICS,
                **INGEST_METRICS,
            },
            'middlemanager': {
                **JETTY_METRICS,
            },
            'peon': {
                **JETTY_METRICS,
                **QUERY_METRICS,
                **QUERY_CACHE_METRICS,
                **QUERY_COUNT_STATS,
                **INGEST_METRICS,
                **SEGMENT_METRICS,
            },
            'realtime': {
                **JETTY_METRICS,
                **QUERY_METRICS,
                **QUERY_CACHE_METRICS,
                **QUERY_COUNT_STATS,
                **INGEST_METRICS,
            },
        }

    def _get_metric_name(self, daemon, metric_name, config):
        if 'name' in config:
            metric_name = config['name']

        log.debug("initial metric_name: {}".format(metric_name))

        metric_name = re.sub('/', r'_', metric_name)
        metric_name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', metric_name)
        metric_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric_name).lower()

        if len(config['suffix']) > 0 and metric_name.endswith(config['suffix']):
            pass
        elif metric_name.endswith('_bytes'):
            pass
        elif metric_name.endswith('_count'):
            pass
        else:
            metric_name = metric_name + config['suffix']

        log.debug("final metric_name: {}".format(metric_name))

        return 'druid_' + daemon + '_' + metric_name

    def process_datapoint(self, datapoint):
        if (datapoint['feed'] != 'metrics'):
            log.debug("'feed' field is not 'metrics' in datapoint, skipping: {}".format(datapoint))
            return

        daemon = str(datapoint['service']).replace('druid/', '').lower()

        if (daemon not in self.supported_metrics):
            log.warn("daemon '{}' is not supported, skipping: {}".format(daemon, datapoint))
            return

        metric_name = str(datapoint['metric'])

        if (metric_name not in self.supported_metrics[daemon]):
            log.warn("metric '{}' is not supported, skipping: {}".format(datapoint['metric'], datapoint))
            return

        config = self.supported_metrics[daemon][metric_name]
        config.setdefault('labels', [])
        config.setdefault('type', 'gauge')
        config.setdefault('suffix', '_count')

        metric_type = config['type']

        if metric_type == 'skip':
            return

        metric_name = self._get_metric_name(daemon, metric_name, config)
        metric_value = float(datapoint['value'])
        metric_labels = tuple(sorted(config['labels'] + ['host']))
        label_values = tuple([datapoint[label_name] for label_name in metric_labels])

        if '_metric_' not in config:
            if metric_type == 'counter':
                config['_metric_'] = Counter(metric_name, metric_name, metric_labels)
            if metric_type == 'gauge':
                config['_metric_'] = Gauge(metric_name, metric_name, metric_labels)
            elif metric_type == 'summary':
                config['_metric_'] = Summary(metric_name, metric_name, metric_labels)
            elif metric_type == 'histogram':
                config['_metric_'] = Histogram(metric_name, metric_name, metric_labels, buckets=config['buckets'])

        metric = config['_metric_']

        if len(metric_labels) > 0:
            metric = metric.labels(*label_values)

        if metric_type == 'counter':
            metric.inc(metric_value)
        if metric_type == 'gauge':
            metric.set(metric_value)
        elif metric_type == 'summary':
            metric.observe(metric_value)
        elif metric_type == 'histogram':
            metric.observe(metric_value)

        self.datapoints_processed.inc()
