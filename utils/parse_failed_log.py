#!/usr/bin/python
# Copyright 2020 Luca Toscano
#                Filippo Giunchedi
#                Wikimedia Foundation
#                Rafa Diaz
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

# All the failed metris are written in the log with a similar format:
#
# DEBUG:druid_exporter.collector:The following datapoint is not supported, either because the 'feed' field is not 'metrics' or the metric itself is not supported: {'feed': 'metrics', 'timestamp': '2020-05-07T09:57:08.983Z', 'service': 'druid/router', 'host': '10.132.0.27:8888', 'version': '0.18.0', 'metric': 'query/time', 'value': 7, 'context': {'queryId': '2c08a196-9c11-49c3-a5e7-b41357388f24', 'timeout': 40000}, 'dataSource': 'daily_page_export', 'duration': 'PT9223372036854775.807S', 'hasFilters': 'false', 'id': '2c08a196-9c11-49c3-a5e7-b41357388f24', 'interval': ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'], 'remoteAddress': '10.0.3.217', 'success': 'true', 'type': 'timeBoundary'}
# 
# [{"feed":"metrics","timestamp":"2020-04-23T11:36:27.866Z","service":"druid/peon","host":"druid1001.eqiad.wmnet:8201",
# "version":"0.12.3","metric":"ingest/events/unparseable","value":0,"dataSource":"wmf_netflow",
# "taskId":["index_kafka_wmf_netflow_ee74c5a5820837c_hedlopdm"]}]
#
# Which should be convert in something like this:
# [..]
#     "peon": {
#     	[..]
#         "ingest/events/unparseable": {
#             "prometheus_metric_name": "druid_realtime_ingest_events_unparseable_count",
#             "type": "gauge",
#             "labels": ["dataSource"],
#             "description": "Number of events rejected because the events are unparseable."
#         },
#         [..]
# [..]
#
# [..]
#     "json['service']": {
#     	[..]
#         "json['metrics']": {
#             "prometheus_metric_name": "druid_+json['metric'].replace('/','_')",
#             "type": "gauge",
#             "labels": ["dataSource"], if json['dataSource"]
#             "description": "put the metric separated with spaces here."
#         },
#         [..]
# [..]
#


import argparse
import json
import logging
import sys
import re

log = logging.getLogger(__name__)


def load_descriptions():
    desc_dict = {}

    fd = open('descriptions.txt', 'r') 
    lines = fd.readlines()
    
    # Strips the newline character 
    for line in lines:
        desc_match = re.match("(.*)\t(.*)",line) 
        if (desc_match):
            # print("Match")
            desc_dict[desc_match.group(1)] = desc_match.group(2)
        else:
            print("No match")
    return desc_dict

def is_processed(service, metric):
    if service not in processed_metrics:
        processed_metrics[service] = []
        processed_metrics[service].append(metric)
        return False
    if metric not in processed_metrics[service]:
        processed_metrics[service].append(metric)
        return False

    return True


p = re.compile("DEBUG:druid_exporter.collector:The following datapoint is not supported, either because the 'feed' field is not 'metrics' or the metric itself is not supported: (.*)")

# Descriptions for metrics are taken from: https://druid.apache.org/docs/latest/operations/metrics.html
descriptions = load_descriptions()
# print(descriptions)

processed_metrics = {}

for line in sys.stdin:
    unsupported_metric = p.search(line)

    if unsupported_metric:
        metric = unsupported_metric.group(1)
        parsed_json = json.loads(metric.replace("'", '"'))
        # print("found: {0}".format(unsupported_metric.group(1)))
        
        if is_processed(parsed_json['service'], parsed_json['metric']):
            continue

        print("\n\n----------------")
        print("Found metric: ")
        print(json.dumps(parsed_json, indent=4, sort_keys=True))
    else:
        continue

    # print("matches: {1}".format(result.group(0)))
    print("\nThis is a metric for: {0}".format(parsed_json['service']))
    output_json = {}
    metric_name = parsed_json['metric']
    output_json[metric_name] = {}
    output_json[metric_name]["prometheus_metric_name"] = "druid_{0}".format(metric_name.replace('/','_'))
    # So far we don't have better way to guess a right description so this can be a temporary solution
    if (metric_name in descriptions.keys()):
        output_json[metric_name]["description"] = descriptions[metric_name]
    else:
        output_json[metric_name]["description"] = "druid_{0}".format(metric_name.replace('/','_'))
    

    # Some heuristic to guess the labels
    possible_labels = ["dataSource", "memKind", "bufferpoolName"]
    output_json[metric_name]['labels'] = [] # By default it needs the field label even with an empty list.

    for possible_label in possible_labels:
        if possible_label in parsed_json.keys():
            print("Adding {0} it as label for prometheus.".format(possible_label))
            output_json[metric_name]['labels'].append(possible_label)

    
    # Some light heuristic to guess the type.
    if re.search("/time$|/bytes$|/cpu$", parsed_json['metric']):
        output_json[metric_name]["type"] = "histogram"
        output_json[metric_name]["buckets"] = [
            "10", "100", "500", "1000", "2000", "3000", "5000", "7000", "10000", "inf", "sum"
        ]
    elif re.search("/count$|/failed$|/size$", parsed_json['metric']):
        output_json[metric_name]["type"] = "gauge"
    else :
        print("Can't guess what type is this metrics: {0} Using Gauge as default type.".format(parsed_json['metric']))
        output_json[metric_name]["type"] = "gauge"
       

    # new_metric = json.dumps(output_json)
    print(json.dumps(output_json, indent=4, sort_keys=True))



