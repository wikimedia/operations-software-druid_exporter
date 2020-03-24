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
import logging

from collections import defaultdict
from pathlib import Path

from prometheus_client.core import (CounterMetricFamily, GaugeMetricFamily,
                                    HistogramMetricFamily, Summary)

log = logging.getLogger(__name__)


class DruidCollector(object):
    scrape_duration = Summary(
        'druid_scrape_duration_seconds', 'Druid scrape duration')

    def __init__(self):
        # Datapoints successfully registered
        self.datapoints_registered = 0

        # List of supported metrics and their fields of the JSON dictionary
        # sent by a Druid daemon. These fields will be added as labels
        # when returning the available metrics in @collect.
        # Due to the fact that metric names are not unique (like segment/count),
        # it is necessary to split the data structure by daemon.
        with open(Path(__file__).parent / 'supported_metrics.json', 'r') as conf:
            self.supported_metric_names = json.load(conf)

        self.counters = defaultdict(lambda: {})

    @staticmethod
    def sanitize_field(datapoint_field):
        return datapoint_field.replace('druid/', '').lower()

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
        daemon = DruidCollector.sanitize_field(str(datapoint['service']))
        metric_name = str(datapoint['metric'])
        metric_value = float(datapoint['value'])

        labels = self.supported_metric_names[daemon][metric_name]
        self.counters.setdefault(daemon, {})
        self.counters[daemon].setdefault(metric_name, {})

        raw_label_values = [datapoint.get(label) for label in labels] if labels else []

        # sometimes labels are wrapped in a list
        cleaned_label_values = (str(v[0] if isinstance(v, list) else v) for v in raw_label_values)

        self.counters[daemon][metric_name][cleaned_label_values] = metric_value

        log.debug("The datapoint {} modified the counters dictionary to: \n{}"
                  .format(datapoint, self.counters))

    @staticmethod
    def _camel_case(s: str):
        return ''.join(['_' + c.lower() if c.isupper() else c for c in s.replace('/', '_')])

    def __get_gauge(self, daemon: str, metric_name: str):
        name = f'druid_{daemon}_{self._camel_case(metric_name)}'
        labelnames = self.supported_metric_names[daemon][metric_name]
        return GaugeMetricFamily(name=name,
                                 documentation='',
                                 labels=labelnames if labelnames else [])

    @scrape_duration.time()
    def collect(self):
        registered = CounterMetricFamily('druid_exporter_datapoints_registered',
                                         'Number of datapoints successfully registered '
                                         'by the exporter.')
        registered.add_metric([], self.datapoints_registered)
        yield registered

        counters = self.counters.copy()
        self.counters.clear()

        # export metrics with actual values
        for daemon in counters:
            for metric in counters[daemon]:
                gauge = self.__get_gauge(daemon, metric)
                for label_values, value in counters[daemon][metric].items():
                    gauge.add_metric(labels=list(label_values),
                                     value=value)
                yield gauge

        # export NaN for metrics without values and no configured labels
        for daemon in self.supported_metric_names:
            for metric in self.supported_metric_names[daemon]:
                if not self.supported_metric_names[daemon][metric] and (
                        not counters.get(daemon) or not counters[daemon].get(metric)):
                    gauge = self.__get_gauge(daemon, metric)
                    gauge.add_metric([], float('nan'))
                    yield gauge

        del counters

    def register_datapoint(self, datapoint):
        if datapoint['feed'] != 'metrics':
            log.debug("The following feed does not contain a datapoint, "
                      "dropping it: {}"
                      .format(datapoint))
            return

        daemon = DruidCollector.sanitize_field(str(datapoint['service']))
        if (datapoint['feed'] != 'metrics' or
                daemon not in self.supported_metric_names or
                datapoint['metric'] not in self.supported_metric_names[daemon]):
            log.debug("The following datapoint is not supported, either "
                      "because the 'feed' field is not 'metrics' or "
                      "the metric itself is not supported: {}"
                      .format(datapoint))
            return

        self.store_counter(datapoint)
        self.datapoints_registered += 1
