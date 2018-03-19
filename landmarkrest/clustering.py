from flask import Blueprint, request, jsonify, abort
from data.models import Cluster
from util.TruffleShuffle import TruffleShuffle
from util.URLClustering import cluster_urls
from settings import CLUSTER_MERGING
import json
import codecs
import logging
import multiprocessing
import requests
import traceback
import random
import sys
import signal

logger = logging.getLogger("landmark")

clustering_api = Blueprint('clustering_api', __name__)

_global_callback = None


def sig_handler(signum, frame):
    if _global_callback:
        data = dict()
        data['status'] = 'ERROR'
        requests.post(_global_callback, json.dumps(data))
signal.signal(signal.SIGSEGV, sig_handler)


@clustering_api.route('/cluster', methods=['POST'])
def cluster(jl_file=None):
    if request and request.method == 'POST':
        data = request.get_json(force=True)
        algorithm = 'URL'
        if 'algorithm' in data:
            algorithm = data['algorithm']

        if 'jl_file' in data:
            jl_file = data['jl_file']
            if 'callback' in data:
                # if the caller provides a callback then run async
                process = multiprocessing.Process(target=cluster_from_jl, args=[jl_file, data['callback']])
                process.start()
                return 'ok'
            else:
                clusters = cluster_from_jl(jl_file, algorithm=algorithm)
                return jsonify(clusters=clusters)

    elif jl_file:
        clusters = cluster_from_jl(jl_file, algorithm='URL')
        return clusters

    return abort(400)


def cluster_from_jl(jl_file, callback=None, limit=None, algorithm='URL'):
    if callback:
        global _global_callback
        _global_callback = callback

    all_clusters = list()
    data = dict()
    try:
        print 'clustering from ' + jl_file + ' with limit of ' + str(limit)
        logging.info('clustering from ' + jl_file + ' with limit of ' + str(limit))
        if algorithm == 'URL':
            url_list = url_list_from_jl_file(jl_file, limit)
            clusters = cluster_urls(url_list)
            cluster_count = 0
            for cluster in clusters:
                cluster_count += 1
                page_ids = []
                for page in cluster[0]:
                    page_ids.append(page[0])
                all_clusters.append(Cluster(cluster_count, page_ids=page_ids, anchor=cluster[1]))
        elif algorithm == 'CONTENT':
            tf = load_jl_file_into_clusterer(jl_file, limit)
            original_clusters = tf.do_truffle_shuffle(algorithm='rule_size')

            if CLUSTER_MERGING:
                train_pages_for_cluster, train_page_map = load_train_pages(jl_file, original_clusters)
                clusters = tf.merge_clusters(original_clusters, train_pages_for_cluster, train_page_map)
            else:
                clusters = original_clusters

            cluster_count = 0
            if len(clusters) > 0:
                for rule in clusters:
                # for rule in sorted(clusters, key=lambda x: len(clusters[x]['MEMBERS']), reverse=True):
                    cluster_count += 1

                    # MEMBERS contains the IDs
                    page_ids = []
                    # print 'cluster' + str(cluster_count) + " -- " + str(len(clusters[rule]['MEMBERS']))
                    for page_id in clusters[rule]['MEMBERS']:
                        page_ids.append(page_id)

                    # generate the Cluster
                    next_cluster = Cluster(cluster_count, page_ids=page_ids, anchor=clusters[rule]['ANCHOR'])
                    all_clusters.append(next_cluster)
            else:
                # generate one cluster from all pages
                next_cluster = Cluster(cluster_count, page_ids=tf.page_chunks_map.keys())
                all_clusters.append(next_cluster)

        if callback:
            from data.encoder import LandmarkJSONEncoder
            data['status'] = 'SUCCESS'
            data['clusters'] = list()
            for one_cluster in all_clusters:
                data['clusters'].append(LandmarkJSONEncoder().default(one_cluster))

    except:
        if callback:
            data['status'] = 'ERROR'
            requests.post(callback, json.dumps(data))
        raise

    if callback:
        requests.post(callback, json.dumps(data))

    return all_clusters


def load_train_pages(jl_file, clusters):
    train_page_map = dict()
    train_pages_for_cluster = dict()
    train_page_ids = list()
    for cluster_rule in clusters:
        # TODO: This 5 is in TruffleShuffle...
        page_ids = clusters[cluster_rule]['MEMBERS']
        random.seed(6)
        train_pages = random.sample(page_ids, 5)
        #print "pages:", train_pages
        train_pages_for_cluster[cluster_rule] = train_pages
        train_page_ids.extend(train_pages)

    if jl_file.startswith('http'):
        response = requests.get(jl_file)
        lines = response.content.splitlines()
    else:
        # start_time_1 = time.time()
        with codecs.open(jl_file, "r", "utf-8") as myfile:
            lines = myfile.readlines()
    for line in lines:
        json_object = json.loads(line)
        page_id = None
        if '_id' in json_object:
            page_id = json_object['_id']
        elif 'doc_id' in json_object:
            page_id = json_object['doc_id']
        if page_id:
            if '_source' in json_object:
                page_str = json_object['_source']['raw_content']
            else:
                page_str = json_object['raw_content']
            if page_id in train_page_ids:
                train_page_map[page_id] = page_str
                #print "add id:", _id
            if len(train_page_map.keys()) == len(train_page_ids):
                #print "return..."
                return train_pages_for_cluster, train_page_map

    return train_pages_for_cluster, train_page_map

def load_jl_file(jl_file):
    jl_info = {}
    if jl_file.startswith('http'):
        response = requests.get(jl_file)
        lines = response.content.splitlines()
    else:
        with codecs.open(jl_file, "r", "utf-8") as myfile:
            lines = myfile.readlines()
    for line in lines:
        json_object = json.loads(line)
        if '_id' in json_object:
            _id = json_object['_id']
            jl_info[_id] = json_object
        elif 'doc_id' in json_object:
            _id = json_object['doc_id']
            jl_info[_id] = json_object
        else:
            print 'ERROR: Document has no ID!'
            logger.error('ERROR: Document has no ID!' + jl_file)
    return jl_info


def load_jl_file_into_clusterer(jl_file, limit=None):
    tf = TruffleShuffle()
    if jl_file.startswith('http'):
        response = requests.get(jl_file)
        lines = response.content.splitlines()
    else:
        # start_time_1 = time.time()
        with codecs.open(jl_file, "r", "utf-8") as myfile:
            lines = myfile.readlines()
        # print "--- myfile.readlines: %.5f seconds ---" % (time.time() - start_time_1)
    # start_time_1 = time.time()
    page_count = 0
    for line in lines:
        json_object = json.loads(line)
        _id = None
        if '_id' in json_object:
            _id = json_object['_id']
        elif 'doc_id' in json_object:
            _id = json_object['doc_id']

        if _id:
            page_str = None
            if '_source' in json_object:
                page_str = json_object['_source']['raw_content']
            else:
                if 'raw_content' in json_object:
                    page_str = json_object['raw_content']

            if page_str:
                tf.add_page(_id, page_str)
                page_count += 1
                if limit and page_count >= limit:
                    break
            else:
                print 'ERROR: Document ' + _id + ' has no raw_content!'
                logger.error('ERROR: Document ' + _id + ' has no raw_content!')
        else:
            print 'ERROR: Document has no ID!'
            logger.error('ERROR: Document has no ID!' + jl_file)

    # print "--- add_pages: %.5f seconds ---" % (time.time() - start_time_1)

    return tf


def url_list_from_jl_file(jl_file, limit=None):
    if jl_file.startswith('http'):
        response = requests.get(jl_file)
        lines = response.content.splitlines()
    else:
        with codecs.open(jl_file, "r", "utf-8") as myfile:
            lines = myfile.readlines()
    page_count = 0
    url_list = []
    seen_ids = []
    for line in lines:
        json_object = json.loads(line)
        page_id = None
        if '_id' in json_object:
            page_id = json_object['_id']
        elif 'doc_id' in json_object:
            page_id = json_object['doc_id']
        if page_id:
            _id = page_id
            if _id not in seen_ids:
                if 'url' in json_object:
                    url_list.append((_id, json_object['url']))
                    seen_ids.append(_id)
                    page_count += 1
                    if limit and page_count >= limit:
                        break
                else:
                    print 'ERROR: Document ' + _id + ' has no URL!'
                    logger.error('ERROR: Document ' + _id + ' has no URL!')
        else:
            print 'ERROR: Document has no ID!'
            logger.error('ERROR: Document has no ID!' + jl_file)

    return url_list



def process_error(message, callback):
    if callback:
        data = dict()
        data['status'] = 'error'
        data['error_message'] = message
        requests.post(callback, json.dumps(data))


def exit_handler(sig, func=None):
    if global_callback:
        process_error("Exit handler triggered with sig " + str(sig), global_callback)

