#!/usr/bin/python
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

import argparse
import json
import logging
import sys

from druid_exporter import collector
from prometheus_client import generate_latest, make_wsgi_app, REGISTRY
from gevent.pywsgi import WSGIServer

log = logging.getLogger(__name__)


class DruidWSGIApp(object):

    def __init__(self, post_uri, druid_collector, prometheus_app, encoding):
        self.prometheus_app = prometheus_app
        self.druid_collector = druid_collector
        self.post_uri = post_uri
        self.encoding = encoding

    def __call__(self, environ, start_response):
        if (environ['REQUEST_METHOD'] == 'GET' and
                environ['PATH_INFO'] == '/metrics'):
            return self.prometheus_app(environ, start_response)
        elif (environ['REQUEST_METHOD'] == 'POST' and
                environ['PATH_INFO'] == self.post_uri and
                environ['CONTENT_TYPE'] == 'application/json'):
            try:
                request_body_size = int(environ.get('CONTENT_LENGTH', 0))
                request_body = environ['wsgi.input'].read(request_body_size)
                datapoints = json.loads(request_body.decode(self.encoding))
                # The HTTP metrics emitter can batch datapoints and send them to
                # a specific endpoint stated in the logs (this tool).
                for datapoint in datapoints:
                    log.debug("Processing datapoint: {}".format(datapoint))
                    self.druid_collector.register_datapoint(datapoint)
                status = '200 OK'
            except Exception as e:
                log.exception('Error while processing the following POST data')
                status = '400 Bad Request'
        else:
            status = '400 Bad Request'
        start_response(status, [])
        return ''


def check_metrics_config_file_consistency(json_config):
    if not json_config:
        raise RuntimeError('Validation error: empty config file.')
    # The first level of nesting should be related to Druid daemon names
    druid_daemon_names = [
        'middlemanager', 'historical', 'peon', 'broker', 'coordinator', 'overlord', 'router']
    required_config_fields = [
        'prometheus_metric_name', 'labels', 'type', 'description'
    ]
    allowed_metric_types = ['histogram', 'counter', 'gauge']
    for daemon in json_config.keys():
        if daemon not in druid_daemon_names:
            raise RuntimeError(
                'Config error: daemon name {} not among the list '
                'of allowed Druid daemons {}.'
                .format(daemon, druid_daemon_names))
        for druid_metric_name in json_config[daemon]:
            metric_metadata = json_config[daemon][druid_metric_name]
            for required_field in required_config_fields:
                if required_field not in metric_metadata.keys():
                    raise RuntimeError(
                        'Config error: metric {} for daemon {} '
                        'misses the required field {}.'
                        .format(druid_metric_name, daemon, required_field))
            if metric_metadata['type'] not in allowed_metric_types:
                raise RuntimeError(
                    'Config error: metric {} for daemon {} has type {}, '
                    'that is not supported. Please use one of {}.'
                    .format(druid_metric_name, daemon,
                            metric_metadata['type'], allowed_metric_type))
            if metric_metadata['type'] == 'histogram' and \
                    'buckets' not in metric_metadata.keys():
                raise RuntimeError(
                    'Config error: metric {} for daemon {} has type {}, '
                    'but no "buckets" field. Please add it to the config.'
                    .format(druid_metric_name, daemon, metric_metadata['type']))


def parse_metrics_config_file(path):
    with open(path, 'r') as f:
        try:
            parsed_json = json.load(f)
        except json.JSONDecodeError as e:
            log.exception(
                'Failed to decode the JSON metric config file '
                'in {}, please check the error reported.'.format(path))
            return {}
        return parsed_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file',
                        help='Config file with the list of metrics to collect/export')
    parser.add_argument('-l', '--listen', metavar='ADDRESS',
                        help='Listen on this address', default=':8000')
    parser.add_argument('-u', '--uri', default='/',
                        help='The URI to check for POSTs coming from Druid')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('-e', '--encoding', default='utf-8',
                        help='Encoding of the Druid POST JSON data.')
    kafka_parser = parser.add_argument_group('kafka',
                                             'Optional configuration for datapoints emitted '
                                             'to a topic via the Druid Kafka Emitter extension.')
    kafka_parser.add_argument('-t', '--kafka-topic',
                              help='Pull datapoints from a given Kafka topic.')
    kafka_parser.add_argument('-b', '--kafka-bootstrap-servers', nargs='+',
                              help='Pull datapoints from a given list of Kafka brokers.')
    kafka_parser.add_argument('-g', '--kafka-consumer-group-id',
                              help='Pull datapoints from Kafka using this Consumer group id.')

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    kafka_args = (args.kafka_topic,
                  args.kafka_bootstrap_servers,
                  args.kafka_consumer_group_id)

    # Check if a Kafka config is provided
    if any(kafka_args):
        if not all(kafka_args):
            argparse.ArgumentParser.error('Kafka configuration incomplete, '
                                          'please provide a topic, one or more brokers '
                                          'as bootstrap-servers and the consumer group id.')
        else:
            kafka_config = {}
            kafka_config['topic'] = args.kafka_topic
            kafka_config['bootstrap_servers'] = args.kafka_bootstrap_servers
            kafka_config['group_id'] = args.kafka_consumer_group_id
            log.info('Using Kafka config: {}'.format(kafka_config))
    else:
        kafka_config = None

    collect_metrics_from = []

    address, port = args.listen.split(':', 1)
    log.info('Starting druid_exporter on %s:%s', address, port)
    log.info('Reading metrics configuration from {}'.format(args.config_file))

    metrics_config = parse_metrics_config_file(args.config_file)
    if metrics_config == {}:
        log.error('Failed to load metrics config file, aborting.')
        return 1

    log.info('Checking consistency of metrics config file..')
    check_metrics_config_file_consistency(metrics_config)

    druid_collector = collector.DruidCollector(
        metrics_config, kafka_config)
    REGISTRY.register(druid_collector)
    prometheus_app = make_wsgi_app()
    druid_wsgi_app = DruidWSGIApp(args.uri, druid_collector,
                                  prometheus_app, args.encoding)

    httpd = WSGIServer(listener=(address, int(port)), application=druid_wsgi_app, log=log)
    httpd.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
