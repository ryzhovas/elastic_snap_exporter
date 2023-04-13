import json
import requests
import prometheus_client as prom
import argparse
import time


class CustomCollector:
    def __init__(self, url):
        self.url = url

    def index_size_stats(self):
        index_info = requests.get(f'{self.url}/_cat/indices?format=json&bytes=b').json()
        index_size_list = []

        for index in index_info:
            if index['pri.store.size'] is None:
                index_size = 0
            else:
                index_size = index['pri.store.size']
            index_size_list.append({'index': index['index'], 'status': index['status'], 'health': index['health'], 'index_size': index_size})
        return index_size_list

    def index_docs_count(self):
        index_info = requests.get(f'{self.url}/_cat/indices?format=json&bytes=b').json()
        index_docs_count = []

        for index in index_info:
            if index['docs.count'] is None:
                index_docs = 0
            else:
                index_docs = index['docs.count']
            index_docs_count.append({'index': index['index'], 'status': index['status'], 'health': index['health'], 'index_docs': index_docs})
        return index_docs_count

    def index_pri_count(self):
        index_info = requests.get(f'{self.url}/_cat/indices?format=json&bytes=b').json()
        index_pri_count = []

        for index in index_info:
            if index['pri'] is None:
                index_pri = 0
            else:
                index_pri = index['pri']
                # print(index_pri)
            index_pri_count.append({'index': index['index'], 'status': index['status'], 'health': index['health'], 'index_pri': index_pri})
        return index_pri_count

    def index_rep_count(self):
        index_info = requests.get(f'{self.url}/_cat/indices?format=json&bytes=b').json()
        index_rep_count = []

        for index in index_info:
            if index['rep'] is None:
                index_rep = 0
            else:
                index_rep = index['rep']
            index_rep_count.append({'index': index['index'], 'status': index['status'], 'health': index['health'], 'index_rep': index_rep})
        return index_rep_count

    def shard_store_size(self):
        shard_info = requests.get(f'{self.url}/_cat/shards?format=json&bytes=b').json()
        shard_store_size = []

        for shard in shard_info:
            if shard['store'] is None:
                shard_size = 0
            else:
                shard_size = shard['store']
            shard_store_size.append({'index': shard['index'], 'state': shard['state'], 'prirep': shard['prirep'], 'shard': shard['shard'], 'shard_size': shard_size})
        return shard_store_size

    def shard_docs_count(self):
        shard_info = requests.get(f'{self.url}/_cat/shards?format=json&bytes=b').json()
        shard_docs_count = []

        for shard in shard_info:
            if shard['docs'] is None:
                shard_docs = 0
            else:
                shard_docs = shard['docs']
            shard_docs_count.append({'index': shard['index'], 'state': shard['state'], 'prirep': shard['prirep'], 'shard': shard['shard'], 'shard_docs': shard_docs})
        return shard_docs_count

    def snap_list(self):
        snap_stats = requests.get(f'{self.url}/_cat/snapshots?format=json&bytes=b').json()
        snap_list = []

        for snap in snap_stats:
            if snap['status'] == 'SUCCESS':
                snap_status = 1
            elif snap['status'] == 'IN_PROGRESS':
                snap_status = 2
            else:
                snap_status = 0
            snap_list.append({'name': snap['id'], 'repository': snap['repository'], 'status': snap['status'], 'indices': snap['indices'],
                              'snap_status': snap_status})
        return snap_list

    def snap_success_shards(self):
        snap_success_shards = requests.get(f'{self.url}/_cat/snapshots?format=json&bytes=b').json()
        snap_success_shards_list = []

        for snap in snap_success_shards:
            snap_success_shards_count = snap['successful_shards']
            snap_success_shards_list.append({'name': snap['id'], 'repository': snap['repository'], 'status': snap['status'], 'indices': snap['indices'],
                              'snap_success_shards_count': snap_success_shards_count})
        return snap_success_shards_list

    def snap_failed_shards(self):
        snap_failed_shards = requests.get(f'{self.url}/_cat/snapshots?format=json&bytes=b').json()
        snap_failed_shards_list = []

        for snap in snap_failed_shards:
            snap_failed_shards_count = snap['failed_shards']
            snap_failed_shards_list.append({'name': snap['id'], 'repository': snap['repository'], 'status': snap['status'], 'indices': snap['indices'],
                              'snap_failed_shards_count': snap_failed_shards_count})
        return snap_failed_shards_list

    def collect(self):
        find_index_size = self.index_size_stats()
        find_index_docs = self.index_docs_count()
        find_index_pri = self.index_pri_count()
        find_index_rep = self.index_rep_count()

        find_shard_size = self.shard_store_size()
        find_shard_docs = self.shard_docs_count()

        find_snap_stats = self.snap_list()
        find_snap_success_shards = self.snap_success_shards()
        find_snap_failed_shards = self.snap_failed_shards()

        keys_index_size = find_index_size[0].keys()
        keys_index_docs = find_index_docs[0].keys()
        keys_index_pri = find_index_pri[0].keys()
        keys_index_rep = find_index_rep[0].keys()

        keys_shard_size = find_shard_size[0].keys()
        keys_shard_docs = find_shard_docs[0].keys()

        keys_snap_stats = find_snap_stats[0].keys()
        keys_snap_success_shards = find_snap_success_shards[0].keys()
        keys_snap_failed_shards = find_snap_failed_shards[0].keys()

        relevant_keys_size = [x for x in keys_index_size if x not in ['index_size']]
        relevant_keys_docs = [x for x in keys_index_docs if x not in ['index_docs']]
        relevant_keys_pri = [x for x in keys_index_pri if x not in ['index_pri']]
        relevant_keys_rep = [x for x in keys_index_rep if x not in ['index_rep']]

        relevant_keys_shard_size = [x for x in keys_shard_size if x not in ['shard_size']]
        relevant_keys_shard_docs = [x for x in keys_shard_docs if x not in ['shard_docs']]

        relevant_keys_snap_stats = [x for x in keys_snap_stats if x not in ['snap_status']]
        relevant_keys_snap_success_shards = [x for x in keys_snap_success_shards if x not in ['snap_success_shards_count']]
        relevant_keys_snap_failed_shards = [x for x in keys_snap_failed_shards if x not in ['snap_failed_shards_count']]

        gauge_index_size = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_index_size', 'custom index size metric',
                                                    labels=relevant_keys_size)
        gauge_docs_count = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_index_docs', 'custom index docs count metric',
                                                    labels=relevant_keys_docs)
        gauge_pri_count = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_index_primaries', 'custom index primaries metric',
                                                               labels=relevant_keys_pri)
        gauge_rep_count = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_index_replicas', 'custom index replicas metric',
                                                               labels=relevant_keys_rep)

        gauge_shard_size = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_shards_size', 'custom shards size metric',
                                                               labels=relevant_keys_shard_size)
        gauge_shard_docs = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_shards_docs', 'custom shards docs metric',
                                                               labels=relevant_keys_shard_docs)

        gauge_snap_stats = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_snap_status',
                                                               'custom snapshot status metric',
                                                               labels=relevant_keys_snap_stats)

        gauge_snap_success_shards = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_snap_shards_success',
                                                               'custom snapshot success shards status metric',
                                                               labels=relevant_keys_snap_success_shards)

        gauge_snap_failed_shards = prom.metrics_core.GaugeMetricFamily('elasticsearch_custom_snap_shards_failed',
                                                               'custom snapshot failed shards status metric',
                                                               labels=relevant_keys_snap_failed_shards)

        for index in find_index_size:
            label_values = []
            for key in keys_index_size:
                label_values.append(str(index[key]))
                # print(label_values)
            gauge_index_size.add_metric(label_values, index['index_size'])
        yield gauge_index_size

        for index in find_index_docs:
            label_values = []
            for key in keys_index_docs:
                label_values.append(str(index[key]))
                # print(label_values)
            gauge_docs_count.add_metric(label_values, index['index_docs'])
        yield gauge_docs_count

        for index in find_index_pri:
            label_values = []
            for key in keys_index_pri:
                label_values.append(str(index[key]))
                # print(label_values)
            gauge_pri_count.add_metric(label_values, index['index_pri'])
        yield gauge_pri_count

        for index in find_index_rep:
            label_values = []
            for key in keys_index_rep:
                label_values.append(str(index[key]))
                # print(label_values)
            gauge_rep_count.add_metric(label_values, index['index_rep'])
        yield gauge_rep_count

        for shard in find_shard_size:
            label_values = []
            for key in keys_shard_size:
                label_values.append(str(shard[key]))
                # print(label_values)
            gauge_shard_size.add_metric(label_values, shard['shard_size'])
        yield gauge_shard_size

        for shard in find_shard_docs:
            label_values = []
            for key in keys_shard_docs:
                label_values.append(str(shard[key]))
                # print(label_values)
            gauge_shard_docs.add_metric(label_values, shard['shard_docs'])
        yield gauge_shard_docs

        for snap in find_snap_stats:
            label_values = []
            for key in keys_snap_stats:
                label_values.append(str(snap[key]))
                # print(label_values)
            gauge_snap_stats.add_metric(label_values, snap['snap_status'])
        yield gauge_snap_stats

        for snap in find_snap_success_shards:
            label_values = []
            for key in keys_snap_success_shards:
                label_values.append(str(snap[key]))
                # print(label_values)
            gauge_snap_success_shards.add_metric(label_values, snap['snap_success_shards_count'])
        yield gauge_snap_success_shards

        for snap in find_snap_failed_shards:
            label_values = []
            for key in keys_snap_failed_shards:
                label_values.append(str(snap[key]))
                # print(label_values)
            gauge_snap_failed_shards.add_metric(label_values, snap['snap_failed_shards_count'])
        yield gauge_snap_failed_shards

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ES connect checker')
    parser.add_argument('-p', '--port', dest="port", help='Listen port', default=9091)
    parser.add_argument('-u', '--url', dest="url", help='URL ES: http://elasticsearch.server:9200',
                        default='http://localhost:9200')
    args = parser.parse_args()

    prom.start_http_server(int(args.port))
    prom.REGISTRY.register(CustomCollector(args.url))
    while True:
        time.sleep(1)
