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
from wsgiref.simple_server import make_server

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--listen', metavar='ADDRESS',
                        help='Listen on this address', default=':8000')
    parser.add_argument('-u', '--uri', default='/',
                        help='The URI to check for POSTs coming from Druid')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('-e', '--encoding', default='utf-8',
                        help='Encoding of the Druid POST JSON data.')
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    collect_metrics_from = []

    address, port = args.listen.split(':', 1)
    log.info('Starting druid_exporter on %s:%s', address, port)

    druid_collector = collector.DruidCollector()
    REGISTRY.register(druid_collector)
    prometheus_app = make_wsgi_app()
    druid_wsgi_app = DruidWSGIApp(args.uri, druid_collector,
                                  prometheus_app, args.encoding)

    httpd = make_server(address, int(port), druid_wsgi_app)
    httpd.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
