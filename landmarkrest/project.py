import requests
import json
from flask import Blueprint, request, jsonify, abort, url_for, Response
from requests.exceptions import Timeout

from database import db_session
from landmarkrest.util.util import Util
from data.models import Harvest, HarvestingStatus, Cluster, Project, ClusteredPage, PageType, Page, Template,\
    Extraction, ProjectStatus, TemplateDebug, Classifier
from sqlalchemy import desc
from sqlalchemy.orm.session import make_transient
from settings import *
from landmark_extractor.extraction.Landmark import flattenResult
import random
import StringIO
import csv
import sys 
from collections import OrderedDict
from util.tld import Tld
import codecs
import os
import multiprocessing
import traceback
import time
from datetime import datetime
import shutil
import logging
import urllib2

project_api = Blueprint('project_api', __name__)
queue = multiprocessing.Queue()

#############################
# GENERAL PROJECT ENDPOINTS #
#############################


@project_api.route('/list', methods=['GET'])
def project_list():
    if request.method == 'GET':
        try:
            db_projects = Project.query.filter(Project.status != ProjectStatus.deleted).all()

            projects = list()
            for project in db_projects:
                projects.append(project.get_view())
            return jsonify(projects=projects)
        except:
            print '[' + datetime.now().isoformat() + '] ERROR with project/list'
            traceback.print_exc()
            abort(500)
    abort(400)


@project_api.route('/create/<string:name>', methods=['POST'])
def create(name):
    if request.method == 'POST' and name:
        project = Project(name)
        db_session.add(project)
        db_session.commit()
        return jsonify(project.get_view())

    abort(400)


@project_api.route('/<int:project_id>/status', methods=['GET'])
def status(project_id):
    if request.method == 'GET' and project_id:
        project = Project.query.filter(Project.id == project_id).first()
        return jsonify(project.get_view())

    abort(400)


@project_api.route('/<int:project_id>/copy/<string:name>', methods=['POST'])
def copy_project(project_id, name):
    if project_id and name:
        project = Project.duplicate_from_db(project_id)
        if project:
            project.name = name
            if project.status == ProjectStatus.published:
                project.status = ProjectStatus.ready
            db_session.add(project)
            db_session.flush()

            # get and copy the harvest for this project
            db_harvest = Harvest.query.filter(Harvest.project_id == project_id).first()
            make_transient(db_harvest)
            db_harvest.id = None
            db_harvest.project_id = project.id
            db_session.add(db_harvest)
            db_session.flush()

            # get and copy the clusters for this project
            clusters = Cluster.query.filter(Cluster.project_id == project_id)
            for cluster in clusters:
                make_transient(cluster)
                cluster.project_id = project.id
                cluster.harvest_id = db_harvest.id
                db_session.add(cluster)
                db_session.flush()

                # get and copy the Clustered Page for this project
                clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id).\
                    filter(ClusteredPage.cluster_id == cluster.cluster_id)
                for clustered_page in clustered_pages:
                    make_transient(clustered_page)
                    clustered_page.project_id = project.id
                    db_session.add(clustered_page)
                    db_session.flush()


                # get and copy the template for this project
                templates = Template.query.filter(Template.project_id == project_id).\
                    filter(Template.cluster_id == cluster.cluster_id)
                for template in templates:
                    make_transient(template)
                    template.id = None
                    template.project_id = project.id
                    db_session.add(template)
                    db_session.flush()

                    # write the new extractions for this project
                    write_cluster_extractions(template)

            db_session.commit()

            return jsonify(project.get_view())

    abort(400)


@project_api.route('/<int:project_id>/delete', methods=['POST'])
def delete_project(project_id):
    if project_id:
        project = Project.query.filter(Project.id == project_id).first()
        previous_status = project.status
        # project.status = ProjectStatus.deleted

        if previous_status == ProjectStatus.published:
            if '-_-' in project.name:
                project_name = project.name.split('-_-')[0]
                project_tld = project.name.split('-_-')[1]
                file_path = os.path.join(LOCAL_S3_DIR, project_name, 'landmark_rules', project_tld + '.json')
                # delete the file if it exists
                if os.path.exists(file_path):
                    os.remove(file_path)
        # project.name = '__deleted__' + project.name
        db_session.delete(project)
        db_session.commit()

        return jsonify(project.get_view())
    abort(400)

#############################
# HARVESTING ENDPOINTS      #
#############################


# Initiates harvesting for a project
@project_api.route('/<int:project_id>/harvest/create', methods=['POST'])
def harvest(project_id, url, email, depth, prefer_pagination, multi_urls, concurrent_requests, 
    concurrent_requests_per_domain, duration, error_page_percentage, error_page_percentage_period):
    try:
        harvest = Harvest(project_id, url, email, depth, prefer_pagination, multi_urls,
            concurrent_requests, concurrent_requests_per_domain, duration, 
            error_page_percentage, error_page_percentage_period, Util.now_millis())

        db_session.add(harvest)
        db_session.commit()

        # Prep harvester config
        crawl_id = HARVEST_PREFIX+str(harvest.id)
        output_file = crawl_id+'.jl'
        callback_url = LANDMARK_URL+'project/'+str(project_id)+'/harvest/callback'
        
        # Check if crawler is being called with multiple start urls
        crawlUrl = ""
        followLinks = True
        if multi_urls:
            crawlUrl = multi_urls
            followLinks = False
        else:
            crawlUrl = url
        
        # Start harvester
        try:
            data = {'url': crawlUrl, 'outputFile': output_file, 'callbackUrl': callback_url,
                    'depthLimit': depth, 'preferPagination': prefer_pagination, 'followLinks': followLinks,
                    'concurrentReq': concurrent_requests, 'concurrentReqPerDomain': concurrent_requests_per_domain, 
                    'duration': duration, 'errorPagePercent': error_page_percentage,
                    'errorPagePercentAfterPeriod': error_page_percentage_period}
            headers = {'Content-type': 'application/json'}
            print('before - ')
            response = requests.post(url=CRAWLER_ENDPOINT+'/crawlerweb/rest/crawler', 
                data=json.dumps(data), headers=headers, timeout=5)
            print('after - ')
            harvest.jl_file_location = S3_CRAWL_BUCKET+output_file
            harvest.crawl_id = crawl_id
            harvest.status = HarvestingStatus.running
            Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.harvesting.value.lower()})
        except:
            harvest.status = HarvestingStatus.unable_to_start
            Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.error.value.lower()})
    
        db_session.commit()
    except:
        traceback.print_exc()
        abort(400)

    return jsonify(harvest.get_view_basic())

# Creates a project and kicks off a harvest in one shot
@project_api.route('/create_harvest', methods=['POST'])
def create_harvest():
    data = request.get_json(silent=True)
    try:
        name = data['name']
        url = data['url']
        email = None
        if 'email' in data:
            email = data['email']
        depth = data['depth']
        prefer_pagination = data['prefer_pagination']
        multi_urls = None
        if 'multi_urls' in data:
            multi_urls = data['multi_urls']
        concurrent_requests = data['concurrent_requests']
        concurrent_requests_per_domain = data['concurrent_requests_per_domain']
        duration = data['duration']
        error_page_percentage = data['error_page_percentage']
        error_page_percentage_period = data['error_page_percentage_period']
        result = json.loads(create(name).data)
        result = harvest(result["id"], url, email, depth, prefer_pagination, multi_urls, concurrent_requests, 
            concurrent_requests_per_domain, duration, error_page_percentage, error_page_percentage_period)
    except:
        traceback.print_exc()
        abort(400)
    return result.data


# Gets a list of all historical harvests for a project
@project_api.route('/<int:project_id>/harvests', methods=['GET'])
def harvest_history(project_id):
    db_harvests = list()
    try:
        db_harvests = Harvest.query.filter(Harvest.project_id==project_id)
        harvests = list()
        for h in db_harvests:
            harvests.append(h.get_view())
    except:
        # traceback.print_exc()
        abort(400)

    return jsonify(harvests)
  
# Gets THE harvest for a project (short term assumption all that projects have ONE harvest)
@project_api.route('/<int:project_id>/harvest', methods=['POST'])
def harvest_info_update(project_id):
    try:
        # force "first()" for now since we assume every project has ONE harvest
        db_harvest = Harvest.query.filter(Harvest.project_id==project_id).first()

        # only query harvester if harvest is not completed
        if db_harvest.completed_ms == None:
            # request status from harvester
            cid = db_harvest.crawl_id
            try:
                response = requests.get(url=CRAWLER_ENDPOINT+'/crawlerweb/rest/crawler/status?crawlerId='+cid,
                                        timeout=1)
            except Timeout:
                return jsonify(db_harvest.get_view())

            json = response.json()
            pages = json['numPagesCrawled']
            endtime = json['endDateTime']

            # update DB record
            db_harvest.pages_fetched = pages
            if endtime > 0:
                db_harvest.completed_ms = endtime
                msg = json['completionMsg']
                if msg == 'Crawler completed successfully':
                  db_harvest.status = HarvestingStatus.completed_success
                else:
                  db_harvest.status = HarvestingStatus.completed_errors
            db_session.commit()        
    except:
        # traceback.print_exc()
        abort(400)

    return jsonify(db_harvest.get_view())


# Callback URL for all crawls, so that resulting JLINES file can be processed/stored
@project_api.route('/<int:project_id>/harvest/callback', methods=['POST'])
def harvest_callback(project_id):
    try:
        # force "first()" for now since we assume every project has ONE harvest
        db_harvest = Harvest.query.filter(Harvest.project_id == project_id).first()

        # parse the remote JLINES file
        response = requests.get(db_harvest.jl_file_location)

        import urllib, cStringIO
        from PIL import Image

        # write a row for every line (page) in the JLINES file
        for ln in response.content.splitlines():
            d = json.loads(ln)
            url = d['url']

            page_id = None
            if '_id' in d:
                page_id = d['_id']
            elif 'doc_id' in d:
                page_id = d['doc_id']

            if 'raw_content' in d:
                html = d['raw_content']
            else:
                continue

            screenshot_url = None
            if 'extracted_metadata' in d:
                meta = d['extracted_metadata']
                if 'screenshot' in meta:
                    screenshot_url = meta['screenshot'].replace('/home/ubuntu/s3/', S3_CRAWL_BUCKET)
            if not screenshot_url and 'metadata' in d:
                meta = d['metadata']
                if 'screenshot' in meta:
                    screenshot_url = meta['screenshot'].replace('/home/ubuntu/s3/', S3_CRAWL_BUCKET)

            thumbnail_url = None
            if screenshot_url:
                page_name = 'h-' + uuid.uuid4().hex + '-' + str(project_id) + 'thumbnail.png'
                thumbnail = Image.open(cStringIO.StringIO(urllib.urlopen(screenshot_url).read()))
                thumbnail.thumbnail((75,75))
                location = LOCAL_S3_DIR + page_name
                thumbnail.save(location)
                thumbnail_url = S3_CRAWL_BUCKET+page_name

            page = Page(db_harvest.id, page_id, html, url, screenshot_url, small_thumbnail_url=thumbnail_url)

            db_session.add(page)

        # TODO: What if this is called twice for some reason?
        db_session.commit()

        # TODO: Maybe use the queue like ISI does?
        if MODE == 'DOCKER_CONTAINER':
            queue.put(project_id)
            time.sleep(1)
        else:
            run_isi_workflow(project_id)
        # cluster_crawl(project_id, db_harvest.id, CLUSTERING_ALGORITHM)
    except:
        import traceback
        traceback.print_exc()
        abort(400)

    return 'ok'

#############################
# CLUSTERING ENDPOINTS      #
#############################


@project_api.route('/<int:project_id>/crawl/<int:crawl_id>/cluster', methods=['POST'])
def cluster_crawl(project_id, crawl_id, algorithm):
    if project_id and crawl_id:
        db_harvest = Harvest.query.filter(Harvest.project_id == project_id, Harvest.id == crawl_id).first()
        callback = url_for('project_api.cluster_callback', project_id=project_id, crawl_id=crawl_id, _external=True)
        data = {"jl_file": db_harvest.jl_file_location, "algorithm": algorithm, "callback": callback}
        requests.post(CLUSTERING_URL+'/cluster', json.dumps(data))
        project = Project.query.filter(Project.id == project_id).first()
        project.status = ProjectStatus.clustering
        db_session.commit()
        return 'ok'

    abort(400)


@project_api.route('/<int:project_id>/crawl/<int:crawl_id>/cluster/callback', methods=['POST'])
def cluster_callback(project_id, crawl_id):
    if project_id and crawl_id:
        data = request.get_json(force=True)
        if 'status' in data:
            if data['status'] == 'error':
                project = Project.query.filter(Project.id == project_id).first()
                project.status = ProjectStatus.error

                db_harvest = Harvest.query.filter(Harvest.project_id == project_id, Harvest.id == crawl_id).first()
                if db_harvest.email:
                    body = 'There was an error clustering ' + str(db_harvest.jl_file_location) + '\n\n'
                    if 'message' in data:
                        body += data['message']

                    Util.send_email(EMAIL_ACCOUNT['username'],
                                    EMAIL_ACCOUNT['password'],
                                    db_harvest.email,
                                    'There was an error clustering ' + project.name,
                                    body)
                db_session.commit()
                return 'ok'

        if 'clusters' in data:
            clusters = data['clusters']
            selected_cluster_id = None
            cluster_db_ids = []
            cluster_count = 0
            page_count = 0
            for new_cluster in clusters:
                anchor = None
                if 'anchor' in new_cluster:
                    anchor = new_cluster['anchor']
                cluster_count += 1
                db_cluster = Cluster(cluster_id=cluster_count,
                                     project_id=project_id, harvest_id=crawl_id, anchor=anchor)
                db_session.add(db_cluster)
                db_session.commit()

                if not selected_cluster_id:
                    selected_cluster_id = db_cluster.cluster_id
                cluster_db_ids.append(db_cluster.cluster_id)

                page_ids = new_cluster['page_ids']
                # "randomly" assign up to 5 pages as train
                random.seed(6)
                train = random.sample(page_ids, min(len(page_ids), 5))
                for page_id in page_ids:
                    page_count += 1
                    page = Page.query.filter(Page.crawl_page_id == page_id).first()

                    page_type = PageType.test
                    if page_id in train:
                        page_type = PageType.train

                    clustered_page = ClusteredPage(project_id, page.id, db_cluster.cluster_id,
                                                   page_type, 'page'+str(page_count))
                    db_session.add(clustered_page)

                db_session.commit()

            if selected_cluster_id:
                # set the cluster for this project
                project = Project.query.filter(Project.id == project_id).first()
                project.selected_cluster_id = selected_cluster_id
                db_session.commit()

            for db_cluster_id in cluster_db_ids:
                # Call learning for each of the clusters
                learn(project_id, crawl_id, db_cluster_id)

            return 'ok'

    abort(400)


@project_api.route('/<int:project_id>/clusters', methods=['GET'])
def get_clusters(project_id):
    if request.method == 'GET' and project_id:
        clusters = list()
        project = Project.query.filter(Project.id == project_id).first()
        db_clusters = Cluster.query.filter(Cluster.project_id == project_id)
        for cluster in db_clusters:
            cluster_view = cluster.get_view(project.selected_cluster_id == cluster.cluster_id)
            pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
                filter(ClusteredPage.cluster_id == cluster.cluster_id)
            if len(pages.all()) > 0:
                for page in pages:
                    real_page = Page.query.filter(Page.id == page.page_id).first()

                    cluster_view.add_page(page.page_id, page.page_type.value.lower(), page.get_name(),
                                          real_page.live_url, real_page.thumbnail_url, real_page.small_thumbnail_url)
                clusters.append(cluster_view)

        return jsonify(clusters)

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/choose', methods=['POST'])
def choose_cluster(project_id, cluster_id):
    if request.method == 'POST' and project_id and cluster_id:
        project = Project.query.filter(Project.id == project_id).first()
        cluster = Cluster.query.filter(Cluster.project_id == project_id). \
            filter(Cluster.cluster_id == cluster_id).first()
        if project and cluster:
            project.choose_cluster(cluster_id)
            db_session.commit()
            return 'ok'
        else:
            abort(404)

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/pages', methods=['GET'])
def get_pages(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:
        pages = list()
        clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
            filter(ClusteredPage.cluster_id == cluster_id).filter(ClusteredPage.page_type != PageType.other). \
            order_by(desc(ClusteredPage.page_type))
        for clustered_page in clustered_pages:
            pages.append(clustered_page.get_view())

        return jsonify(pages)

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/pages/move/<string:page_type>', methods=['POST'])
def move_pages(project_id, cluster_id, page_type):
    if request.method == 'POST' and project_id and cluster_id and page_type:
        data = request.get_json(force=True)
        if 'page_ids' in data:
            template = Template.get_from_db(project_id, cluster_id)

            page_ids = data['page_ids']
            clustered_pages =\
                ClusteredPage.query.join(Page, Page.id == ClusteredPage.page_id).\
                filter(Page.id.in_(page_ids)).filter(ClusteredPage.project_id == project_id).all()

            cluster_dirty = False

            for clustered_page in clustered_pages:
                clustered_page.cluster_id = cluster_id

                if page_type == PageType.train.value:
                    # we are moving these into train so we should set the markup for them
                    update_page_extraction_and_markup(clustered_page.page_id, template)
                    cluster_dirty = True
                elif clustered_page.page_type.value == PageType.train.value and page_type != PageType.train.value:
                    update_page_extraction_and_markup(clustered_page.page_id, template, delete_markup=True)
                    cluster_dirty = True

                clustered_page.page_type = PageType(page_type)

            if cluster_dirty:
                cluster = Cluster.query.filter(Cluster.project_id == project_id). \
                    filter(Cluster.cluster_id == cluster_id).first()
                cluster.dirty = True

            db_session.commit()

            write_cluster_extractions(template)

            db_session.commit()

            return 'ok'

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/merge/<int:old_cluster_id>', methods=['POST'])
def merge_clusters(project_id, cluster_id, old_cluster_id):
    if request.method == 'POST' and project_id and cluster_id and old_cluster_id:
        old_clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
            filter(ClusteredPage.cluster_id == old_cluster_id)
        template = Template.get_from_db(project_id, cluster_id)
        cluster_dirty = False
        for clustered_page in old_clustered_pages:
            clustered_page.cluster_id = cluster_id
            if clustered_page.page_type == PageType.train:
                update_page_extraction_and_markup(clustered_page.page_id, template)
                cluster_dirty = True
            elif clustered_page.page_type == PageType.test:
                update_page_extraction_and_markup(clustered_page.page_id, template, delete_markup=True)

        if cluster_dirty:
            Cluster.query.filter(Cluster.project_id == project_id). \
                filter(Cluster.cluster_id == cluster_id).update({"dirty": True})

        # Then update extractions for this cluster and "delete" the other cluster
        Cluster.query.filter(Cluster.project_id == project_id).filter(Cluster.cluster_id == old_cluster_id) \
            .delete(synchronize_session=False)
        Template.query.filter(Template.project_id == project_id).filter(Template.cluster_id == old_cluster_id) \
            .delete(synchronize_session=False)
        TemplateDebug.query.filter(TemplateDebug.project_id == project_id). \
            filter(TemplateDebug.cluster_id == old_cluster_id). \
            delete(synchronize_session=False)
        Extraction.query.filter(Extraction.project_id == project_id). \
            filter(Extraction.cluster_id == old_cluster_id). \
            delete(synchronize_session=False)
        db_session.flush()

        write_cluster_extractions(template)

        db_session.commit()

        return 'ok'

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>', methods=['GET'])
def get_cluster_info(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:
        return 'ok'

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/data/grid', methods=['GET'])
def get_grid_data(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:

        template = Template.get_from_db(project_id, cluster_id)
        fields = template.get_flattened_schema_view()

        pages = list()
        clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
            filter(ClusteredPage.cluster_id == cluster_id).filter(ClusteredPage.page_type != PageType.other). \
            order_by(desc(ClusteredPage.page_type))
        for clustered_page in clustered_pages:
            grid_data_view = clustered_page.get_view()
            extraction = Extraction.query.filter(Extraction.cluster_id == cluster_id). \
                filter(Extraction.project_id == project_id).filter(Extraction.template_id == template.id). \
                filter(Extraction.page_id == clustered_page.page_id).first()

            for field in fields:
                grid_data_view.add_field_values(field['schemaid'], extraction.get_values(field['schemaid']))

            pages.append(grid_data_view)

        return jsonify(fields=fields, pages=pages)

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/reset', methods=['POST'])
def reset_cluster(project_id, cluster_id):
    if request.method == 'POST' and project_id and cluster_id:
        project = Project.query.filter(Project.id == project_id).first()
        if project:
            # if there is a supervised version then delete it and it's extractions
            supervised_template = Template.query.filter(Template.project_id == project_id). \
                filter(Template.cluster_id == cluster_id). \
                filter(Template.supervised.is_(True))

            template = supervised_template.first()
            if template:
                Extraction.query.filter(Extraction.template_id == template.id).delete()
                supervised_template.delete()
                db_session.commit()

            db_cluster = Cluster.query.filter(Cluster.project_id == project_id). \
                filter(Cluster.cluster_id == cluster_id).first()
            db_cluster.classifier = Classifier.default
            if db_cluster.dirty:
                learn(project_id, 999, cluster_id)
                db_cluster.dirty = False
            db_session.commit()

        return jsonify(project.get_view())

    abort(400)

#############################
# LEARNING ENDPOINTS        #
#############################


def get_current_template(project_id, cluster_id):
    template = Template.get_from_db(project_id, cluster_id)

    if not template.supervised:
        supervised_template = Template.duplicate_from_db(project_id, cluster_id)
        # then we should make a supervised one... and use it
        supervised_template.supervised = True

        db_session.add(supervised_template)
        db_session.commit()

        write_cluster_extractions(supervised_template)
        return supervised_template
    else:
        return template


def update_page_extraction_and_markup(page_id, template, delete_markup=False):
    page = Page.query.filter(Page.id == page_id).first()

    json_extractions = template.apply_template(page)
    extraction = Extraction(template.project_id, template.cluster_id, template.id, page.id,
                            json.dumps(json_extractions, indent=2, separators=(',', ': ')))
    if not delete_markup:
        template.set_markup_object(page_id, json_extractions)
    else:
        template.set_markup_object(page_id, None)
    db_session.merge(extraction)
    db_session.flush()


def write_cluster_extractions(template):
    """Helper function to write extractions for all pages in a cluster to DB for a template"""
    # Then store the extractions in the database while we are at it
    query = db_session.query(Page).join(ClusteredPage, Page.id == ClusteredPage.page_id)
    all_pages_results = query.filter(ClusteredPage.project_id == template.project_id) \
        .filter(ClusteredPage.cluster_id == template.cluster_id)

    for page in all_pages_results:
        json_extractions = template.apply_template(page)
        extraction = Extraction(template.project_id, template.cluster_id, template.id, page.id,
                                json.dumps(json_extractions, indent=2, separators=(',', ': ')))
        db_session.merge(extraction)
        db_session.flush()


@project_api.route('/<int:project_id>/crawl/<int:crawl_id>/cluster/<int:cluster_id>/learn', methods=['POST'])
def learn(project_id, crawl_id, cluster_id):
    """Function to learn for a given project_id, crawl_id and cluster_id"""
    if project_id and crawl_id and cluster_id:
        print 'Learning for ' + str(cluster_id) + ' in project ' + str(project_id)
        Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.learning.value.lower()})
        query = db_session.query(Page).join(ClusteredPage, Page.id == ClusteredPage.page_id)
        pages_results = query.filter(ClusteredPage.project_id == project_id) \
            .filter(ClusteredPage.cluster_id == cluster_id).filter(ClusteredPage.page_type == PageType.train)

        pages = {}
        for page in pages_results:
            pages[page.id] = page.html

        callback = url_for('project_api.learn_callback',
                           project_id=project_id,
                           crawl_id=crawl_id,
                           cluster_id=cluster_id,
                           _external=True)
        data = {'pages': pages, 'callback': callback}
        requests.post(LEARNING_URL+'/unsupervised', json.dumps(data))

        db_session.commit()
        return 'ok'

    abort(400)


@project_api.route('/<int:project_id>/crawl/<int:crawl_id>/cluster/<int:cluster_id>/learn/callback', methods=['POST'])
def learn_callback(project_id, crawl_id, cluster_id):
    if project_id and crawl_id and cluster_id:
        data = json.loads(request.get_data(), object_pairs_hook=OrderedDict)
        if 'template' in data:
            json_template = data['template']

            # Look up this template and store in the database
            template = db_session.query(Template). \
                filter(Template.project_id == project_id). \
                filter(Template.cluster_id == cluster_id). \
                filter(Template.supervised.is_(False)). \
                one_or_none()

            if template:
                template.stripes = json.dumps(json_template['stripes'], indent=2)
                template.rules = json.dumps(json_template['rules'], indent=2)
                template.markup = json.dumps(json_template['markup'], indent=2)
                template.set_schema_view()
            else:
                template = Template(stripes=json.dumps(json_template['stripes'], indent=2),
                                    rules=json.dumps(json_template['rules'], indent=2),
                                    markup=json.dumps(json_template['markup'], indent=2),
                                    supervised=False)
                template.project_id = project_id
                template.cluster_id = cluster_id
                template.set_schema_view()
                db_session.add(template)
            db_session.commit()
            write_cluster_extractions(template)

            # add the debug information for each template
            if 'debug_htmls' in data:
                debug_htmls = data['debug_htmls']
                for page_id in debug_htmls:
                    template_debug = TemplateDebug(project_id, cluster_id, template.id, page_id, debug_htmls[page_id])
                    db_session.merge(template_debug)
                db_session.commit()

            # count the templates and does it == # of clusters?
            # if so then say this is ready and send an email
            template_rows = Template.query.filter(Template.project_id == project_id).\
                filter(Template.supervised.is_(False)).count()
            cluster_rows = Cluster.query.filter(Cluster.project_id == project_id).count()
            if template_rows == cluster_rows:
                Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.ready.value.lower()})
                db_harvest = Harvest.query.filter(Harvest.project_id == project_id).first()

                if db_harvest.email:
                    link = request.url_root + '#/project/' + str(project_id) + '/cluster'
                    link = link.replace('5000', '3333')
                    body = 'Go to ' + link + ' review the data.'

                    Util.send_email(EMAIL_ACCOUNT['username'],
                                    EMAIL_ACCOUNT['password'],
                                    db_harvest.email,
                                    'Your Landmark Data Is Ready To Review!',
                                    body)
                db_session.commit()

            return 'ok'

    abort(400)


# # @project_api.route('/<int:project_id>/cluster/<int:cluster_id>/learn/unsupervised', methods=['POST'])
# def learn_unsupervised(project_id, cluster_id):
#     if project_id and cluster_id:
#         # Load data from database
#         query = db_session.query(Page).join(ClusteredPage, Page.id == ClusteredPage.page_id)
#         pages_results = query.filter(ClusteredPage.project_id == project_id) \
#             .filter(ClusteredPage.cluster_id == cluster_id).filter(ClusteredPage.page_type == PageType.train)
#         pages = {}
#         for page in pages_results:
#             pages[page.id] = page.html
#
#         # Post to learning
#         data = {'pages': pages}
#         response = requests.post(url=LANDMARK_URL+'learning/unsupervised', data=json.dumps(data))
#         return_data = response.json()
#         json_template = return_data['template']
#
#         # Look up this template and store in the database
#         template = db_session.query(Template). \
#             filter(Template.project_id == project_id). \
#             filter(Template.cluster_id == cluster_id). \
#             filter(Template.supervised.is_(False)). \
#             one_or_none()
#
#         if template:
#             template.stripes = json.dumps(json_template['stripes'], indent=2)
#             template.rules = json.dumps(json_template['rules'], indent=2)
#             template.markup = json.dumps(json_template['markup'], indent=2)
#         else:
#             template = Template(stripes=json.dumps(json_template['stripes'], indent=2),
#                                 rules=json.dumps(json_template['rules'], indent=2),
#                                 markup=json.dumps(json_template['markup'], indent=2),
#                                 supervised=False)
#             template.project_id = project_id
#             template.cluster_id = cluster_id
#             db_session.add(template)
#         db_session.commit()
#         write_cluster_extractions(template)
#
#         # add the debug information for each template
#         if 'debug_htmls' in return_data:
#             debug_htmls = return_data['debug_htmls']
#             for page_id in debug_htmls:
#                 template_debug = TemplateDebug(project_id, cluster_id, template.id, page_id, debug_htmls[page_id])
#                 db_session.merge(template_debug)
#             db_session.commit()
#
#     #     return jsonify(json_template)
#     # abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/train', methods=['POST'])
def learn_supervised(project_id, cluster_id):
    if request.method == 'POST' and project_id and cluster_id:
        # Load data from database
        query = db_session.query(Page).join(ClusteredPage, Page.id == ClusteredPage.page_id)
        pages_results = query.filter(ClusteredPage.project_id == project_id) \
            .filter(ClusteredPage.cluster_id == cluster_id) \
            .filter(ClusteredPage.page_type == PageType.train.value.lower())
        pages = {}
        for page in pages_results:
            pages[page.id] = page.html

        old_template = Template.get_from_db(project_id, cluster_id)
        if not old_template.supervised:
            return jsonify(old_template)

        # else let's take that markup and try to learn
        markup = json.loads(old_template.markup, object_pairs_hook=OrderedDict)

        # Post to learning
        data = {'pages': pages, 'markup': markup}
        response = requests.post(url=LANDMARK_URL+'learning/supervised', data=json.dumps(data))
        return_data = response.json()
        json_template = return_data['template']

        # Look up this template and store it in the database
        template = db_session.query(Template). \
            filter(Template.project_id == project_id). \
            filter(Template.cluster_id == cluster_id). \
            filter(Template.supervised.is_(True)). \
            one_or_none()

        if template:
            template.update_template(json_template['stripes'],
                                     json_template['rules'],
                                     json_template['markup']
                                     )
            db_session.commit()
            write_cluster_extractions(template)

            # add the debug information for the new template
            if 'debug_htmls' in return_data:
                debug_htmls = return_data['debug_htmls']
                for page_id in debug_htmls:
                    template_debug = TemplateDebug(project_id, cluster_id, template.id, page_id, debug_htmls[page_id])
                    db_session.merge(template_debug)
                db_session.commit()

            return jsonify(template)
    abort(400)

#############################
# MARKUP ENDPOINTS        #
#############################


# def build_schema_view(db_fields):
#     """Helper function to build the schema view for the front end from the items returned from the DB"""
#     fields = list()
#     for field in db_fields:
#         field_view = field.get_view()
#         sub_fields = build_schema_view(field.children)
#         field_view.add_sub_fields(sub_fields)
#         fields.append(field_view)
#
#     return fields


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/schema', methods=['GET'])
def get_schema(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:
        template = Template.get_from_db(project_id, cluster_id)

        return jsonify(json.loads(template.schema, object_pairs_hook=OrderedDict))

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/page/<int:page_id>/markup', methods=['PUT'])
def set_markup(project_id, cluster_id, page_id):
    if request.method == 'PUT' and project_id and cluster_id and page_id:
        data = request.get_json(force=True)
        if 'markup' in data:
            markup = data['markup']
            template = get_current_template(project_id, cluster_id)

            extraction = Extraction.query.filter(Extraction.page_id == page_id). \
                filter(Extraction.template_id == template.id). \
                first()

            updated_markup = template.update_markup_from_extraction_view(page_id, markup)
            db_session.commit()

            schema_object = json.loads(template.schema, object_pairs_hook=OrderedDict)
            extraction_view = extraction.get_fix_view(updated_markup, schema_object[0]['list'])

            return jsonify(extraction_view)

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/page/<int:page_id>', methods=['GET'])
def get_page_extractions(project_id, cluster_id, page_id):
    if request.method == 'GET' and project_id and cluster_id and page_id:
        template = Template.get_from_db(project_id, cluster_id)
        extraction = Extraction.query.filter(Extraction.page_id == page_id). \
            filter(Extraction.project_id == project_id). \
            filter(Extraction.cluster_id == cluster_id). \
            filter(Extraction.template_id == template.id). \
            first()

        markup_object = None
        if template.markup:
            str_page_id = str(page_id)
            whole_markup = json.loads(template.markup, object_pairs_hook=OrderedDict)
            if str_page_id in whole_markup:
                markup_object = whole_markup[str_page_id]

        schema_object = json.loads(template.schema, object_pairs_hook=OrderedDict)
        extraction_view = extraction.get_fix_view(markup_object, schema_object[0]['list'])

        return jsonify(extraction_view)
    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/page/<int:page_id>/cached', methods=['GET'])
def get_cached_page(project_id, cluster_id, page_id):
    if request.method == 'GET' and project_id and cluster_id and page_id:
        page = Page.query.join(ClusteredPage, Page.id == ClusteredPage.page_id) \
            .filter(ClusteredPage.project_id == project_id).filter(ClusteredPage.cluster_id == cluster_id) \
            .filter(Page.id == page_id).first()
        # page = Page.query.filter(Page.id == page_id).first()
        if page:
            return page.html
    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/field/<string:field_id>/rename/<string:new_name>',
                   methods=['PUT'])
def rename_field(project_id, cluster_id, field_id, new_name):
    if request.method == 'PUT' and project_id and cluster_id and field_id and new_name:
        template = get_current_template(project_id, cluster_id)

        updated_id = template.update_field(field_id, new_name)
        if updated_id:
            db_session.flush()
            write_cluster_extractions(template)
            db_session.commit()
            return jsonify(json.loads(template.schema, object_pairs_hook=OrderedDict))
        else:
            abort(400, 'Unable to rename ' + field_id)

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/field/<string:field_id>/delete', methods=['DELETE'])
def delete_field(project_id, cluster_id, field_id):
    if request.method == 'DELETE' and project_id and cluster_id and field_id:
        template = get_current_template(project_id, cluster_id)

        updated_id = template.update_field(field_id)
        if updated_id:
            db_session.flush()
            write_cluster_extractions(template)
            db_session.commit()
            return jsonify(json.loads(template.schema, object_pairs_hook=OrderedDict))
        else:
            abort(400, 'Unable to delete ' + field_id)

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/field/add', methods=['PUT'])
def add_field(project_id, cluster_id):
    if request.method == 'PUT' and project_id and cluster_id:
        data = request.get_json(force=True)
        if 'name' in data and 'type' in data:
            template = get_current_template(project_id, cluster_id)

            parent_id = None
            if 'parent_id' in data and data['parent_id'] != '0':
                parent_id = data['parent_id']

            new_schemaid = template.add_field(data['name'], data['type'], parent_id)
            if new_schemaid:
                db_session.commit()
                return jsonify(new_schemaid)
            abort(400, 'Unable to add ' + data['name'])

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/page/<int:page_id>/rename/<string:page_name>',
                   methods=['POST'])
def rename_page(project_id, cluster_id, page_id, page_name):
    if request.method == 'POST' and project_id and cluster_id and page_id and page_name:
        clustered_page = ClusteredPage.query.filter(ClusteredPage.project_id == project_id).\
            filter(ClusteredPage.cluster_id == cluster_id).\
            filter(ClusteredPage.page_id == page_id).first()
        clustered_page.set_alias(page_name)
        db_session.commit()
        return 'ok'

    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/page/<int:page_id>/html/cached', methods=['GET'])
def get_page_html_cached(project_id, cluster_id, page_id):
    if request.method == 'GET' and project_id and cluster_id and page_id:
        page = Page.query.filter(Page.id == page_id).first()
        return Response(page.html, mimetype='text/html')
    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/page/<int:page_id>/html/debug', methods=['GET'])
def get_page_html_debug(project_id, cluster_id, page_id):
    if request.method == 'GET' and project_id and cluster_id and page_id:
        template = Template.get_from_db(project_id, cluster_id)
        template_debug = TemplateDebug.query.filter(TemplateDebug.project_id == project_id).\
            filter(TemplateDebug.cluster_id == cluster_id).\
            filter(TemplateDebug.template_id == template.id).\
            filter(TemplateDebug.page_id == page_id).first()
        return Response(template_debug.debug_html, mimetype='text/html')
    abort(400)

#############################
# DELIVER ENDPOINTS         #
#############################


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/data/<string:data_format>', methods=['GET'])
def get_data(project_id, cluster_id, data_format):
    if request.method == 'GET' and project_id and cluster_id and data_format:
        template = Template.get_from_db(project_id, cluster_id)
        extractions = Extraction.query.filter(Extraction.project_id == project_id). \
            filter(Extraction.cluster_id == cluster_id). \
            filter(Extraction.template_id == template.id)

        if data_format == 'json':
            return_data = {}
            for extraction in extractions:
                return_data[extraction.page_id] = json.loads(extraction.extraction_json)
            return jsonify(return_data)
        elif data_format == 'rules':
            db_harvest = Harvest.query.filter(Harvest.project_id == project_id).first()
            rules_metadata = dict()
            rules_metadata['tld'] = Tld.extract_tld(db_harvest.url)
            rules = json.loads(template.rules, object_pairs_hook=OrderedDict)
            return jsonify(metadata=rules_metadata, rules=rules)
        elif data_format == 'csv':
            data_to_be_processed = []
            for extraction in extractions:
                flattened_json = flattenResult(json.loads(extraction.extraction_json))
                clustered_page = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
                    filter(ClusteredPage.cluster_id == cluster_id). \
                    filter(ClusteredPage.page_id == extraction.page_id).first()
                flattened_json['__PAGENAME__'] = clustered_page.alias
                data_to_be_processed.append(flattened_json)
            processed_data = []
            header = []
            for item in data_to_be_processed:
                reduced_item = {}
                Util.reduce_item(reduced_item, None, item)

                header += reduced_item.keys()

                processed_data.append(reduced_item)

            header = list(set(header))
            header.sort()

            si = StringIO.StringIO()
            writer = csv.DictWriter(si, header, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in processed_data:
                writer.writerow(row)
            output = si.getvalue()

            return Response(output, mimetype='text/csv')

    abort(400)


@project_api.route('/<int:project_id>/extract', methods=['GET'])
def extract_for_url(project_id):
    project = Project.query.filter(Project.id == project_id).first()
    dl_url = request.args.get('url')
    if project and dl_url:
        cluster_id = project.selected_cluster_id
        if cluster_id:
            # get the template which has the rules then apply them and return the JSON
            template = get_current_template(project_id, cluster_id)
            if template:
                # get the HTML string
                req = urllib2.Request(dl_url,
                                      headers={'User-Agent': "Magic Browser"})
                con = urllib2.urlopen(req)
                html_str = con.read()
                print html_str

                from landmark_extractor.extraction.Landmark import RuleSet
                json_object = json.loads(template.rules, object_pairs_hook=OrderedDict)
                rules = RuleSet(json_object)
                json_extractions = flattenResult(rules.extract(html_str))
                return jsonify(json_extractions)
    abort(400)


class HarvestHistory(object):

    def __init__(self, history_id, crawl_token):
        self.id = history_id
        self.crawl_token = crawl_token


############################
## Validation EndPoints ####
############################

from util.Validation import Validation
from util.Validation import ValidationRule
from data.models import ValidationTable
import copy



def get_validation_from_project(project_id, cluster_id):
    validations_for_schema = ValidationTable.get_schema_validations(project_id, cluster_id)
    validations = OrderedDict()
    for v in validations_for_schema:
        if v.field_id in validations:
            # add to existing field
            validation_types = validations[v.field_id]
        else:
            validation_types = list()
            validations[v.field_id] = validation_types

        one_validation = OrderedDict()
        one_validation['type'] = v.validation_type
        one_validation['param'] = v.validation_param
        validation_types.append(one_validation)

    return validations

@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/default_classifier', methods=['PUT'])
def set_default_classifier(project_id, cluster_id):
    if request.method == 'PUT' and project_id and cluster_id:
        cluster = Cluster.query.filter(Cluster.project_id == project_id).filter(Cluster.cluster_id == cluster_id).first()
        cluster.classifier = Classifier.default
        db_session.commit()
        return "ok"
    abort(400)

@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/rule_classifier', methods=['PUT'])
def set_rule_classifier(project_id, cluster_id):
    if request.method == 'PUT' and project_id and cluster_id:
        cluster = Cluster.query.filter(Cluster.project_id == project_id).filter(Cluster.cluster_id == cluster_id).first()
        cluster.classifier = Classifier.rule
        db_session.commit()
        return "ok"
    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/get_classifier', methods=['GET'])
def get_classifier(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:
        return Cluster.query.filter(Cluster.project_id == project_id).filter(Cluster.cluster_id == cluster_id).first().classifier.value
    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/data/verify/mapped', methods=['GET'])
def verify_mapped(project_id, cluster_id):
    template = Template.get_from_db(project_id, cluster_id)
    all_fields = template.get_flattened_schema_view()
    fields = list()
    rule_ids_to_run = list()
    for field in all_fields:
        if 'mapped' in field and field['mapped']:
            fields.append(field)
            rule_ids_to_run.append(field['schemaid'])

    validation_query = ValidationTable.query.filter(ValidationTable.project_id == project_id). \
        filter(ValidationTable.cluster_id == cluster_id)
    # get all pages for this query
    clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
        order_by(ClusteredPage.cluster_id)
    pages = list()

    default_clustering = (Cluster.query.filter(Cluster.project_id == project_id). \
                          filter(Cluster.cluster_id == cluster_id).first().classifier == Classifier.default)

    for clustered_page in clustered_pages:
        grid_data_view = clustered_page.get_view()
        page = Page.query.filter(Page.id == clustered_page.page_id).first()
        json_extractions = template.apply_template(page, rule_ids_to_run=rule_ids_to_run)
        extraction = Extraction(template.project_id, template.cluster_id, template.id, page.id,
                                json.dumps(json_extractions, indent=2, separators=(',', ': ')))
        grid_data_view.cluster_id = clustered_page.cluster_id

        grid_data_view.valid = 1
        valid_extractions = 0
        for field in fields:
            grid_data_view.add_field_values(field['schemaid'], extraction.get_values(field['schemaid']))
            if default_clustering and len(extraction.get_values(field['schemaid'])) > 0 and \
                    extraction.get_values(field['schemaid'])[0]['value']:
                valid_extractions += 1
            elif not default_clustering:
                rule = validation_query.filter(ValidationTable.field_id == field['schemaid']).first()
                if rule:
                    validation_rule = ValidationRule(rule.validation_type, rule.validation_param)
                    if not validation_rule.execute_rule(json.loads(extraction.extraction_json)[field['name']]):
                        grid_data_view.valid = 0

        if default_clustering and (valid_extractions < len(fields) / 2.0):
            grid_data_view.valid = 0
        pages.append(grid_data_view)

    return jsonify(fields=fields, pages=pages)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/data/verify/in', methods=['GET'])
def verify_in_cluster(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:

        template = Template.get_from_db(project_id, cluster_id)
        fields = template.get_flattened_schema_view()
        validation_query = ValidationTable.query.filter(ValidationTable.project_id == project_id). \
            filter(ValidationTable.cluster_id == cluster_id)
        clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
            filter(ClusteredPage.cluster_id == cluster_id).filter(ClusteredPage.page_type != PageType.other). \
            order_by(desc(ClusteredPage.page_type))
        pages = list()

        default_clustering = (Cluster.query.filter(Cluster.project_id == project_id). \
                              filter(Cluster.cluster_id == cluster_id).first().classifier == Classifier.default)

        for clustered_page in clustered_pages:
            grid_data_view = clustered_page.get_view()
            extraction = Extraction.query.filter(Extraction.cluster_id == cluster_id). \
                filter(Extraction.project_id == project_id).filter(Extraction.template_id == template.id). \
                filter(Extraction.page_id == clustered_page.page_id).first()
            grid_data_view.cluster_id = clustered_page.cluster_id

            grid_data_view.valid = 1
            valid_extractions = 0
            for field in fields:
                grid_data_view.add_field_values(field['schemaid'], extraction.get_values(field['schemaid']))
                if default_clustering and len(extraction.get_values(field['schemaid'])) > 0 and \
                        extraction.get_values(field['schemaid'])[0]['value']:
                    valid_extractions += 1
                elif not default_clustering:
                    rule = validation_query.filter(ValidationTable.field_id == field['schemaid']).first()
                    if rule:
                        validation_rule = ValidationRule(rule.validation_type, rule.validation_param)
                        if not validation_rule.execute_rule(json.loads(extraction.extraction_json)[field['name']]):
                            grid_data_view.valid = 0

            if default_clustering and (valid_extractions < len(fields) / 2.0):
                grid_data_view.valid = 0
            pages.append(grid_data_view)

        return jsonify(fields=fields, pages=pages)
    abort(400)


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/data/verify/out', methods=['GET'])
def verify_out_of_cluster(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:

        template = Template.get_from_db(project_id, cluster_id)
        fields = template.get_flattened_schema_view()
        validation_query = ValidationTable.query.filter(ValidationTable.project_id == project_id). \
            filter(ValidationTable.cluster_id == cluster_id)
        clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
            filter(ClusteredPage.cluster_id != cluster_id).filter(ClusteredPage.page_type != PageType.other). \
            order_by(desc(ClusteredPage.page_type))
        pages = list()

        default_clustering = (Cluster.query.filter(Cluster.project_id == project_id). \
                              filter(Cluster.cluster_id == cluster_id).first().classifier == Classifier.default)

        for clustered_page in clustered_pages:
            grid_data_view = clustered_page.get_view()
            page = Page.query.filter(Page.id == clustered_page.page_id).first()
            json_extractions = template.apply_template(page)
            extraction = Extraction(template.project_id, template.cluster_id, template.id, page.id,
                                    json.dumps(json_extractions, indent=2, separators=(',', ': ')))
            grid_data_view.cluster_id = clustered_page.cluster_id

            grid_data_view.valid = 1
            valid_extractions = 0
            for field in fields:
                grid_data_view.add_field_values(field['schemaid'], extraction.get_values(field['schemaid']))
                if default_clustering and len(extraction.get_values(field['schemaid'])) > 0 and \
                        extraction.get_values(field['schemaid'])[0]['value']:
                    valid_extractions += 1
                elif not default_clustering:
                    rule = validation_query.filter(ValidationTable.field_id == field['schemaid']).first()
                    if rule:
                        validation_rule = ValidationRule(rule.validation_type, rule.validation_param)
                        if not validation_rule.execute_rule(json.loads(extraction.extraction_json)[field['name']]):
                            grid_data_view.valid = 0

            if default_clustering and (valid_extractions < len(fields) / 2.0):
                grid_data_view.valid = 0
            pages.append(grid_data_view)

        return jsonify(fields=fields, pages=sorted(pages, key= lambda view: view.cluster_id))
    abort(400)


#get all validations for schema
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/schema/validation', methods=['GET'])
def get_validation_for_schema(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:
        validations = get_validation_from_project(project_id, cluster_id)
        return jsonify(validation=validations)

    abort(400)

#get schema with validation
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/validation/schema', methods=['GET'])
def get_schema_with_validation(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:
        template = Template.get_from_db(project_id, cluster_id)
        schema_json = json.loads(template.schema, object_pairs_hook=OrderedDict)
        validations = get_validation_from_project(project_id, cluster_id)
        v = Validation(validations)
        schema_with_validation = v.get_schema_with_validation(schema_json)
        return jsonify(schema_with_validation)

    abort(400)

#add one validation rule for field
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/field/validation/add', methods=['PUT'])
def add_validation_for_field(project_id, cluster_id):
    if request.method == 'PUT' and project_id and cluster_id:
        data = request.get_json(force=True)
        if 'fieldid' in data and 'validation' in data:
            validation_param = None
            if 'param' in data['validation'][0]:
                validation_param = data['validation'][0]['param']
            new_validation = ValidationTable(project_id, cluster_id, data['fieldid'],
                                             data['validation'][0]['type'], validation_param)
            db_session.add(new_validation)
            from sqlalchemy import exc
            try:
                db_session.commit()
            except exc.IntegrityError as e:
                print 'validation for field already exists'

        return "ok"
    abort(400)

#delete one validation rule for field
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/field/validation/delete', methods=['PUT'])
def delete_validation_for_field(project_id, cluster_id):
    if request.method == 'PUT' and project_id and cluster_id:
        data = request.get_json(force=True)
        if 'fieldid' in data and 'validation' in data:
            validation_param = None
            field_id = data['fieldid']
            validation_type = data['validation'][0]['type']
            if 'param' in data['validation'][0]:
                validation_param = data['validation'][0]['param']
                validation_for_delete = ValidationTable.query.filter(ValidationTable.project_id == project_id). \
                    filter(ValidationTable.cluster_id == cluster_id).filter(ValidationTable.field_id == field_id). \
                    filter(ValidationTable.validation_type == validation_type). \
                    filter(ValidationTable.validation_param == validation_param).first()
            else:
                validation_for_delete = ValidationTable.query.filter(ValidationTable.project_id == project_id). \
                    filter(ValidationTable.cluster_id == cluster_id).filter(ValidationTable.field_id == field_id). \
                    filter(ValidationTable.validation_type == validation_type).first()

            db_session.delete(validation_for_delete)
            db_session.commit()

        return "ok"
    abort(400)

#get validation info for field extraction
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/field/extraction/validation', methods=['PUT'])
def get_validation_for_field_extraction(project_id, cluster_id):
    if request.method == 'PUT' and project_id and cluster_id:
        data = request.get_json(force=True)
        if 'fieldid' in data:
            validations = get_validation_from_project(project_id, cluster_id)
            v = Validation(validations)
            #v.get_validations_from_db(project_id, cluster_id)
            # validate
            v.validate_field_extraction(data)
            return jsonify(data)

    abort(400)

#reset cluster and delete all validation info for that cluster
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/validation/reset', methods=['POST'])
def reset_cluster_with_validation(project_id, cluster_id):
    if request.method == 'POST' and project_id and cluster_id:
        project = Project.query.filter(Project.id == project_id).first()
        if project:
            # if there is a supervised version then delete it and it's extractions
            supervised_template = Template.query.filter(Template.project_id == project_id). \
                filter(Template.cluster_id == cluster_id). \
                filter(Template.supervised.is_(True))

            template = supervised_template.first()
            if template:
                Extraction.query.filter(Extraction.template_id == template.id).delete()
                supervised_template.delete()
                db_session.commit()

            db_cluster = Cluster.query.filter(Cluster.project_id == project_id). \
                filter(Cluster.cluster_id == cluster_id).first()
            db_cluster.classifier = Classifier.default
            if db_cluster.dirty:
                learn(project_id, 999, cluster_id)
                db_cluster.dirty = False
            db_session.commit()

            #delete all validation for given cluster
            ValidationTable.query.filter(ValidationTable.project_id == project_id). \
                filter(ValidationTable.cluster_id == cluster_id).delete()
            project.status = ProjectStatus.ready
            db_session.commit()

        return jsonify(project.get_view())

    abort(400)


# get extraction with validation in FIX view
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/page/<int:page_id>/validation', methods=['GET'])
def get_page_extractions_with_validation(project_id, cluster_id, page_id):
    if request.method == 'GET' and project_id and cluster_id and page_id:
        template = Template.get_from_db(project_id, cluster_id)
        extraction = Extraction.query.filter(Extraction.page_id == page_id). \
            filter(Extraction.project_id == project_id). \
            filter(Extraction.cluster_id == cluster_id). \
            filter(Extraction.template_id == template.id). \
            first()

        # get extraction with NO validation
        #import copy
        extraction_json = json.loads(extraction.extraction_json, object_pairs_hook=OrderedDict)
        extraction_for_page_with_validation = copy.deepcopy(extraction_json)
        validations = get_validation_from_project(project_id, cluster_id)
        v = Validation(validations)
        # validate
        v.validate_page(extraction_for_page_with_validation)
        # add extraction with validation to database
        #print json.dumps(extraction_for_page_with_validation, indent=2, separators=(',', ': '))
        extraction.update_extraction_with_validation(
            json.dumps(extraction_for_page_with_validation, indent=2, separators=(',', ': ')))
        db_session.commit()

        markup_object = None
        if template.markup:
            str_page_id = str(page_id)
            whole_markup = json.loads(template.markup, object_pairs_hook=OrderedDict)
            if str_page_id in whole_markup:
                markup_object = whole_markup[str_page_id]

        schema_object = json.loads(template.schema, object_pairs_hook=OrderedDict)
        extraction_view = extraction.get_fix_view_with_validation(markup_object, schema_object[0]['list'])

        return jsonify(extraction_view)
    abort(400)

#get flattened data with validation for Verify View
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/data/grid/validation', methods=['GET'])
def get_grid_data_with_validation(project_id, cluster_id):
    if request.method == 'GET' and project_id and cluster_id:
        limit = request.args.get('limit')

        template = Template.get_from_db(project_id, cluster_id)
        fields = template.get_flattened_schema_view()

        pages = list()
        if limit:
            clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
                filter(ClusteredPage.cluster_id == cluster_id).filter(ClusteredPage.page_type != PageType.other). \
                order_by(desc(ClusteredPage.page_type)).limit(limit)
        else:
            clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == project_id). \
                filter(ClusteredPage.cluster_id == cluster_id).filter(ClusteredPage.page_type != PageType.other). \
                order_by(desc(ClusteredPage.page_type))
        extractions = Extraction.query.filter(Extraction.cluster_id == cluster_id). \
                filter(Extraction.project_id == project_id).filter(Extraction.template_id == template.id)
        for clustered_page in clustered_pages:
            grid_data_view = clustered_page.get_view()
            extraction = extractions.filter(Extraction.page_id == clustered_page.page_id).first()
            # validate extraction
            validations = get_validation_from_project(project_id, cluster_id)
            v = Validation(validations)
            # get extraction with NO validation
            extraction_for_page_with_validation = json.loads(extraction.extraction_json, object_pairs_hook=OrderedDict)
            #extraction_for_page_with_validation = copy.deepcopy(extraction_json)
            v.validate_page(extraction_for_page_with_validation)
            # use functional API; this will not work because it takes as input an entire extraction,
            # not only one page
            # data = {'extraction': extraction, 'validation': validation}
            # response = requests.post(EXTRACTION_URL + '/validation', json.dumps(data))
            # return_data = response.json()
            # extraction_with_validation = return_data['extraction']
            ##################
            # add extraction with validation to database
            extraction.update_extraction_with_validation(json.dumps(extraction_for_page_with_validation, indent=2, separators=(',', ': ')))
            db_session.commit()

            for field in fields:
                grid_data_view.add_field_values(field['schemaid'],
                                                extraction.get_values_with_validation(field['schemaid'], field['name']))
            pages.append(grid_data_view)

        return jsonify(fields=fields, pages=pages)

    abort(400)

#add validation to JSON extraction (may not be needed)
@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/data/<string:data_format>/validation', methods=['GET'])
def get_data_with_validation(project_id, cluster_id, data_format):
    if request.method == 'GET' and project_id and cluster_id and data_format:
        template = Template.get_from_db(project_id, cluster_id)
        extractions = Extraction.query.filter(Extraction.project_id == project_id).filter(
            Extraction.cluster_id == cluster_id) \
            .filter(Extraction.template_id == template.id)

        return_data = None
        if data_format == 'json':
            return_data = OrderedDict()
            validations = get_validation_from_project(project_id, cluster_id)
            v = Validation(validations)
            for extraction in extractions:
                # get extraction with NO validation
                extraction_json = json.loads(extraction.extraction_json, object_pairs_hook=OrderedDict)
                # validate extraction
                extraction_for_page_with_validation = copy.deepcopy(extraction_json)
                v.validate_page(extraction_for_page_with_validation)
                # add extraction with validation to database
                extraction.update_extraction_with_validation(
                    json.dumps(extraction_for_page_with_validation, indent=2, separators=(',', ': ')))
                db_session.commit()

                return_data[extraction.page_id] = json.loads(extraction.extraction_json_with_validation,
                                                             object_pairs_hook=OrderedDict)

            return jsonify(return_data)
        elif data_format == 'csv':
            # do not include validation with csv data
            data_to_be_processed = []
            for extraction in extractions:
                flattened_json = flattenResult(json.loads(extraction.extraction_json, object_pairs_hook=OrderedDict))
                flattened_json['__PAGENAME__'] = extraction.page_id
                data_to_be_processed.append(flattened_json)
            processed_data = []
            header = []
            for item in data_to_be_processed:
                reduced_item = {}
                Util.reduce_item(reduced_item, None, item)

                header += reduced_item.keys()

                processed_data.append(reduced_item)

            header = list(set(header))
            header.sort()

            si = StringIO.StringIO()
            writer = csv.DictWriter(si, header, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in processed_data:
                writer.writerow(row)
            output = si.getvalue()

            return Response(output, mimetype='text/csv')

    abort(400)

###################################
# ISI "SPECIAL" ENDPOINTS         #
###################################


@project_api.route('/<int:project_id>/cluster/<int:cluster_id>/data/publish', methods=['POST'])
def publish(project_id, cluster_id):
    project = Project.query.filter(Project.id == project_id).first()
    template = Template.get_from_db(project_id, cluster_id)
    db_harvest = Harvest.query.filter(Harvest.project_id == project_id).first()
    if '-_-' in project.name:
        project_name = project.name.split('-_-')[0]
        project_tld = project.name.split('-_-')[1]
    else:
        project_name = project.name
        project_tld = Tld.extract_tld(db_harvest.url)
    rules_metadata = dict()
    rules_metadata['tld'] = Tld.extract_tld(db_harvest.url)
    rules_metadata['task_name'] = project.name

    all_fields = template.get_flattened_schema_view()
    rule_ids_to_run = list()
    for field in all_fields:
        if 'mapped' in field and field['mapped']:
            rule_ids_to_run.append(field['schemaid'])

    template_rules = json.loads(template.rules, object_pairs_hook=OrderedDict)
    rules = list()
    for rule in template_rules:
        if rule['id'] in rule_ids_to_run:
            rules.append(rule)

    rules_file = dict()
    rules_file['metadata'] = rules_metadata
    rules_file['rules'] = rules
    rules_content = json.dumps(rules_file, indent=2)

    if LOCAL_S3_DIR:
        file_path = os.path.join(LOCAL_S3_DIR, project_name, 'landmark_rules', project_tld + '.json')
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path))
        with codecs.open(file_path, "w", "utf-8") as myfile:
            myfile.write(rules_content)

        Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.published.value.lower()})
        db_session.commit()
        return 'ok'
    return abort(500)


@project_api.route('/<int:project_id>/field_names', methods=['GET'])
def isi_field_names(project_id):
    return jsonify(get_isi_field_names(project_id))


def get_isi_field_names(project_id):
    project = Project.query.filter(Project.id == project_id).first()
    project_name = project.name.split('-_-')[0]

    field_names = []
    if project_id and ISI_FIELDS_ENDPOINT:
        try:
            headers = {}
            if FIELDS_ENDPOINT_AUTH_TOKEN:
                headers['Authorization'] = FIELDS_ENDPOINT_AUTH_TOKEN

            endpoint = ISI_FIELDS_ENDPOINT.replace('{{project_name}}', project_name)
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                return_data = response.json()
                field_names = return_data.keys()
            else:
                field_names = ['name', 'price', 'website', 'title', 'date']
        except:
            field_names = ['name', 'price', 'website', 'title', 'date']
    return field_names


@project_api.route('/<int:project_id>/missing_thumbnails', methods=['POST'])
def populate_missing_thumbnails(project_id):
    print 'Getting missing thumbnails for ... ' + str(project_id)
    # query = ClusteredPage.query.join(Page, ClusteredPage.page_id == Page.id)
    # all_pages_results = query.filter(ClusteredPage.project_id == project_id).filter(Page.thumbnail_url == None)

    all_pages_results = \
        ClusteredPage.query.join(Page, Page.id == ClusteredPage.page_id) \
        .filter(ClusteredPage.project_id == project_id).filter(Page.thumbnail_url.is_(None)).all()

    all_pages_results = \
        ClusteredPage.query.join(Page, Page.id == ClusteredPage.page_id) \
        .filter(ClusteredPage.project_id == project_id).filter(Page.thumbnail_url.is_(None)) \
        .filter(ClusteredPage.page_type == PageType.train.value.lower()) \
        .all()

    for page in all_pages_results:
        page_id = page.page_id
        page_name = 'h-'+uuid.uuid4().hex+'-'+str(project_id)+'.png'
        cluster_id = page.cluster_id
        if SERVER_URL:
            cached_page = (SERVER_URL
                        + '/project/'+str(project_id)
                        + '/cluster/'+str(cluster_id)
                        + '/page/'+str(page_id)
                        + '/html/cached'
                          )
        else:
            cached_page = url_for('project_api.get_cached_page', project_id=project_id,
                                  cluster_id=cluster_id, page_id=page_id, _external=True)
        thumbnail_url = IMAGE_GEN_ENDPOINT + cached_page
        try:
            # urllib.urlretrieve(thumbnail_url, LOCAL_S3_DIR+page_name)
            local_location = LOCAL_S3_DIR+page_name
            if dlfile(thumbnail_url, local_location):
                Page.query.filter(Page.id == page_id).update({"thumbnail_url": S3_CRAWL_BUCKET+page_name})
                db_session.commit()
        except IOError:
            print('Skipping ' + str(page_id) + '. Thumbnail service may be down. Please check '+SERVER_URL)
    print 'Finished getting missing thumbnails for ... ' + str(project_id)
    return 'ok'


@project_api.route('/export/<string:prefix>', methods=['POST'])
def export_projects(prefix):
    real_prefix = prefix + '-_-'
    projects = Project.query.filter(Project.name.startswith(real_prefix))\
        .filter(Project.status != ProjectStatus.deleted).all()
    projects_json = []
    for project in projects:
        project_json = project.export_to_json()
        projects_json.append(project_json)

    return jsonify(projects_json)


@project_api.route('/<int:project_id>/export', methods=['POST'])
def export_project(project_id):
    project = Project.query.filter(Project.id == project_id).first()
    project_json = project.export_to_json()
    return jsonify(project_json)


@project_api.route('/import/<string:project_name>', methods=['POST'])
def import_projects(project_name):
    # delete all projects with the project_name
    real_prefix = project_name + '-_-'
    projects = Project.query.filter(Project.name.startswith(real_prefix))\
        .filter(Project.status != ProjectStatus.deleted).all()
    for project in projects:
        # project.status = ProjectStatus.deleted
        # project.name = '__deleted__' + project.name
        db_session.delete(project)
        db_session.commit()

    new_projects = []

    data = request.get_json(force=True)
    if data:
        if isinstance(data, dict):
            (new_project, templates) = Project.import_from_json(data, project_name)
            for template in templates:
                write_cluster_extractions(template)
                db_session.commit()
                new_projects.append(new_project.get_view())
        elif isinstance(data, list):
            for project_object in data:
                try:
                    (new_project, templates) = Project.import_from_json(project_object, project_name)
                    for template in templates:
                        write_cluster_extractions(template)
                        db_session.commit()
                    new_projects.append(new_project.get_view())
                except:
                    print 'ERROR!!!'

    return jsonify(new_projects)


def dlfile(url, local_location):
    try:
        time.sleep(1)
        print '[' + datetime.now().isoformat() + '] sending request to: ' + str(url)
        response = None
        wait_time = 3
        tries = 2
        response_status = -1
        while tries > 0 and response_status != 200:
            response = requests.get(url, stream=True, timeout=10)
            response_status = response.status_code
            print '[' + datetime.now().isoformat() + '] ' + str(response.status_code) + ': ' + str(response.reason)
            if response_status == 200:
                break
            else:
                print 'dlfile ERROR: retry in ' + str(wait_time) + 's'
                if 'Retry-After' in response.headers:
                    print 'dlfile ERROR: Retry-After set to ' + str(response.headers['Retry-After']) + 's in header'
                tries = tries - 1
                time.sleep(wait_time)
                del response

        if response_status == 200:
            with open(local_location, 'wb') as out_file:
                for block in response.iter_content(1024):
                    if not block:
                        break
                    out_file.write(block)
                # shutil.copyfileobj(response.raw, out_file)
            print '[' + datetime.now().isoformat() + '] png written to ' + local_location
            del response
            return True

    # handle errors
    except:
        traceback.print_exc()

    return False


def run_isi_workflow(project_id, write_pages=False, get_debug_htmls=True):
    start_time_whole = time.time()

    db_harvest = Harvest.query.filter(Harvest.project_id == project_id).first()
    page_count = 0
    with_urls = 0

    if write_pages:
        start_time_1 = time.time()
        # parse the remote JLINES file
        jl_file = db_harvest.jl_file_location
        if jl_file.startswith('http'):
            response = requests.get(jl_file)
            lines = response.content.splitlines()
        else:
            with codecs.open(jl_file, "r", "utf-8") as myfile:
                lines = myfile.readlines()

        # write a row for every line (page) in the JLINES file
        seen_ids = []
        for ln in lines:
            d = json.loads(ln)

            url = 'NOT_PROVIDED'
            if 'url' in d:
                url = d['url']
                with_urls += 1

            page_id = None
            if '_id' in d:
                page_id = d['_id']
            elif 'doc_id' in d:
                page_id = d['doc_id']

            if 'raw_content' in d:
                html = d['raw_content']
            else:
                continue

            screenshot_url = None
            if 'extracted_metadata' in d:
                meta = d['extracted_metadata']
                if 'screenshot' in meta:
                    screenshot_url = meta['screenshot'].replace('/home/ubuntu/s3/', S3_CRAWL_BUCKET)
            if page_id not in seen_ids:
                page = Page(db_harvest.id, page_id, html, url, screenshot_url)
                db_session.add(page)
                seen_ids.append(page_id)
                page_count += 1
                if page_count >= PAGES_PER_SITE:
                    break

        print "project %d: -- read remote file and write pages to DB: %.5f seconds --" %\
              (project_id,(time.time() - start_time_1))
        logging.info("project %d: -- read remote file and write pages to DB: %.5f seconds --" %
                     (project_id, (time.time() - start_time_1)))

    start_time_1 = time.time()

    Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.clustering.value.lower()})
    db_session.commit()

    from clustering import cluster_from_jl
    crawl_id = db_harvest.id

    clustering_algorithm = CLUSTERING_ALGORITHM
    if page_count > 0:
        percent_without = ((1.0 * (page_count - with_urls)) / page_count) * 100.0
        logging.info('percent_without = ' + str(percent_without))
        if percent_without >= 0.95:
            clustering_algorithm = 'CONTENT'

    clusters = cluster_from_jl(db_harvest.jl_file_location, limit=PAGES_PER_SITE, algorithm=clustering_algorithm)

    if not clusters:
        Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.error.value.lower()})
        db_session.commit()
        return

    selected_cluster_id = None
    cluster_db_ids = []
    cluster_count = 0
    page_count = 0
    for new_cluster in clusters:
        page_ids = new_cluster.page_ids
        if len(page_ids) < 5:
            continue
        anchor = new_cluster.anchor
        cluster_count += 1
        db_cluster = Cluster(cluster_id=cluster_count,
                             project_id=project_id, harvest_id=crawl_id, anchor=anchor)
        db_session.add(db_cluster)
        db_session.commit()

        if not selected_cluster_id:
            selected_cluster_id = db_cluster.cluster_id
        cluster_db_ids.append(db_cluster.cluster_id)

        # "randomly" assign up to 5 pages as train
        random.seed(6)
        train = random.sample(page_ids, 5)
        for page_id in page_ids:
            page_count += 1
            page = Page.query.filter(Page.crawl_page_id == page_id).filter(Page.crawl_id == crawl_id).first()

            if page:
                page_type = PageType.test
                if page_id in train:
                    page_type = PageType.train

                clustered_page = ClusteredPage(project_id, page.id, db_cluster.cluster_id,
                                               page_type, 'page' + str(page_count))
                db_session.add(clustered_page)

        db_session.commit()

    Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.learning.value.lower()})

    if selected_cluster_id:
        # set the cluster for this project
        project = Project.query.filter(Project.id == project_id).first()
        project.selected_cluster_id = selected_cluster_id
        db_session.commit()
    else:
        Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.error.value.lower()})
        db_session.commit()
        return

    print "project %d: -- clustering: %.5f seconds --" % (project_id, (time.time() - start_time_1))
    logging.info("project %d: -- clustering: %.5f seconds --" % (project_id, (time.time() - start_time_1)))

    start_time_1 = time.time()

    # # if any of the pages are missing thumbnails then go process them:
    if IMAGE_GEN_ENDPOINT:
        populate_missing_thumbnails(project_id)
        print "-- populate missing thumbnails: %.5f seconds --" % (time.time() - start_time_1)

    start_time_1 = time.time()

    from learning import TemplateFactory

    for db_cluster_id in cluster_db_ids:
        try:
            print '[' + datetime.now().isoformat() + '] Learning for ' + str(db_cluster_id) + ' in project ' + str(project_id)
            logging.info('[' + datetime.now().isoformat() + '] Learning for ' + str(db_cluster_id) + ' in project ' + str(project_id))
            query = db_session.query(Page).join(ClusteredPage, Page.id == ClusteredPage.page_id)
            pages_results = query.filter(ClusteredPage.project_id == project_id) \
                .filter(ClusteredPage.cluster_id == db_cluster_id).filter(ClusteredPage.page_type == PageType.train)

            pages = {}
            for page in pages_results:
                pages[page.id] = page.html
            # Call learning for each of the clusters
            start_time_2 = time.time()
            learned_template, debug_htmls = \
                TemplateFactory.unsupervised_learning(pages,
                                                      learn_lists=UNSUPERVISED_LEARN_LISTS,
                                                      debug_template=get_debug_htmls,
                                                      field_prediction=False,
                                                      remove_bad_rules=False)

            if learned_template:
                template = Template(stripes=json.dumps(learned_template.stripes, indent=2),
                                    rules=learned_template.rules,
                                    markup=learned_template.markup,
                                    supervised=False)
                template.project_id = project_id
                template.cluster_id = db_cluster_id
                db_session.add(template)
                db_session.flush()
                write_cluster_extractions(template)
                db_session.commit()

            if debug_htmls:
                for page_id in debug_htmls:
                    template_debug = TemplateDebug(
                        project_id, db_cluster_id, template.id, page_id, debug_htmls[page_id])
                    db_session.add(template_debug)
                db_session.commit()
            print '[%s] -- Learning for %d in project %d took %.5f seconds --' % \
                  (datetime.now().isoformat(), db_cluster_id, project_id, time.time() - start_time_2)
            print '[' + datetime.now().isoformat() + '] Done learning for ' + str(db_cluster_id) + ' in project ' + str(project_id)
            logging.info('[%s] -- Learning for %d in project %d took %.5f seconds --' %
                         (datetime.now().isoformat(), db_cluster_id, project_id, time.time() - start_time_2))
            logging.info('[' + datetime.now().isoformat() + '] Done learning for ' + str(db_cluster_id) + ' in project ' + str(project_id))
        except:
            traceback.print_exc()
            print '[' + datetime.now().isoformat() + '] Error learning for ' + str(db_cluster_id) + ' in project ' + str(project_id)
            logging.info('[' + datetime.now().isoformat() + '] Error learning for ' + str(db_cluster_id) + ' in project ' + str(project_id))

    print "project %d: -- learning for all clusters: %.5f seconds --" % (project_id, (time.time() - start_time_1))
    logging.info("project %d: -- learning for all clusters: %.5f seconds --" % (project_id, (time.time() - start_time_1)))

    Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.ready.value.lower()})

    print "project %d: --- whole workflow: %.5f seconds ---" % (project_id, (time.time() - start_time_whole))
    logging.info("project %d: --- whole workflow: %.5f seconds ---" % (project_id, (time.time() - start_time_whole)))

    db_session.commit()

#
# @project_api.route('/create_from_jl_file/tld/<string:tld>/name/<string:project_name>', methods=['POST'])
# def create_from_jl_file(tld, project_name):
#     if request.data:
#         data = request.get_data()
#         if data:
#             data = data.decode('utf-8')
#             now = time.gmtime()
#
#             print 'Creating project for ... ', tld
#
#             # Create a Project
#             project = Project(project_name + '-_-' + tld + "-" + time.strftime("%Y%m%d_%H%M%S", now))
#             db_session.add(project)
#             db_session.flush()
#
#             depth = -1
#             prefer_pagination = False
#             multi_urls = ""
#             concurrent_requests = -1
#             concurrent_requests_per_domain = -1
#             duration = -1
#             error_page_percentage = -1
#             error_page_percentage_period = -1
#
#             # Create a Harvest
#             db_harvest = Harvest(project.id, 'http://' + tld, '', depth, prefer_pagination, multi_urls,
#                                  concurrent_requests, concurrent_requests_per_domain, duration, error_page_percentage,
#                                  error_page_percentage_period, Util.now_millis())
#             db_session.add(db_harvest)
#             db_session.commit()
#
#             db_harvest.crawl_id = ISI_HARVEST_PREFIX + str(db_harvest.id)
#             crawl_id = ISI_HARVEST_PREFIX + str(db_harvest.id)
#             output_file = crawl_id + '-' + project_name + '-_-' + tld + '.jl'
#             local_file = os.path.join(LOCAL_S3_DIR, output_file)
#             page_count = -1
#             with codecs.open(local_file, "w", "utf-8") as myfile:
#                 myfile.write(data)
#             db_harvest.pages_fetched = page_count
#             db_harvest.jl_file_location = S3_CRAWL_BUCKET + output_file
#             db_session.commit()
#
#             project_ids = list()
#             project_ids.append(project.id)
#             print 'Created project!'
#             process = multiprocessing.Process(target=resume_create_from_es_worker,
#                                               args=[project_ids])
#             process.start()
#             return 'ok'


@project_api.route('/<int:project_id>/copy_noname', methods=['POST'])
def copy_no_name(project_id):
    project = Project.query.filter(Project.id == project_id).first()
    project_name = project.name.split(' ')[0]
    same_name_projects = Project.query.filter(Project.name.like(project_name+'%')).all()
    new_name = project_name + ' copy' + str(len(same_name_projects))
    return copy_project(project_id, new_name)


@project_api.route('/<int:project_id>/restart_isi_workflow', methods=['POST'])
def restart_isi_workflow(project_id):
    # TODO: Clear the status and tables then run this one
    queue.put(project_id)
    return 'ok'


@project_api.route('/create_from_dig/<string:project_name>', methods=['POST'])
def create_from_dig(project_name):
    if request.data:
        data = request.get_json(force=True)
        if data:
            project_ids = []
            now = time.gmtime()
            for tld in data.iterkeys():
                try:
                    project_id = create_project_from_dig(project_name, tld, data[tld], now)
                except:
                    print 'error with tld: ' + tld
                    traceback.print_exc()
                    logging.error('error with tld: ' + tld)
                    logging.error(traceback.format_exc())
                if project_id:
                    project_ids.append(project_id)

            for project_id in project_ids:
                queue.put(project_id)
                time.sleep(1)
        return 'ok'
    abort(400)


def create_project_from_dig(project_name, tld, post_data, now=None):
    project_id = None
    if tld:
        print 'Creating project for ... ', tld

        # Create a Project
        if not now:
            now = time.gmtime()
        project = Project(project_name + '-_-' + tld + "-" + time.strftime("%Y%m%d_%H%M%S", now))
        db_session.add(project)
        db_session.flush()

        # Create a Harvest
        depth = -1
        prefer_pagination = False
        multi_urls = ""
        concurrent_requests = -1
        concurrent_requests_per_domain = -1
        duration = -1
        error_page_percentage = -1
        error_page_percentage_period = -1

        # Create a Harvest
        db_harvest = Harvest(project.id, 'http://'+tld, '', depth, prefer_pagination, multi_urls,
                             concurrent_requests, concurrent_requests_per_domain, duration, error_page_percentage,
                             error_page_percentage_period, Util.now_millis())
        db_session.add(db_harvest)
        db_session.flush()
        db_harvest.crawl_id = ISI_HARVEST_PREFIX + str(db_harvest.id)

        jl_file_location = None
        page_count = 0
        if 'jl_file' in post_data:
            jl_file_location = post_data['jl_file']
            page_count = file_len(jl_file_location)
        elif 'documents' in post_data:
            # then make a jl_file
            output_file = db_harvest.crawl_id+'-'+project_name+'-_-'+tld+'.jl'
            jl_file_location = os.path.join(LOCAL_S3_DIR, project_name, 'working_dir', output_file)

            if not os.path.exists(os.path.dirname(jl_file_location)):
                os.makedirs(os.path.dirname(jl_file_location))

            with codecs.open(jl_file_location, "w", "utf-8") as jl_file:
                seen_ids = []
                for document_object in post_data['documents']:
                    page_id = None
                    if '_id' in document_object:
                        page_id = document_object['_id']
                    elif 'doc_id' in document_object:
                        page_id = document_object['doc_id']

                    if page_id not in seen_ids:
                        if 'raw_content' in document_object:
                            jl_file.write(json.dumps(document_object) + "\n")
                            seen_ids.append(page_id)
                            page_count += 1
                        elif 'raw_content_path' in document_object:
                            document_raw_content_path = document_object['raw_content_path']
                            if document_raw_content_path.startswith('http'):
                                req = urllib2.Request(document_raw_content_path,
                                                      headers={'User-Agent': "Magic Browser"})
                                con = urllib2.urlopen(req)
                                html_str = con.read()
                            else:
                                with codecs.open(document_raw_content_path, "r", "utf-8") as html_file:
                                    html_str = html_file.read()
                            document_object['raw_content'] = html_str
                            jl_file.write(json.dumps(document_object) + "\n")
                            seen_ids.append(page_id)
                            page_count += 1

        db_harvest.pages_fetched = page_count
        db_harvest.jl_file_location = jl_file_location
        db_session.add(db_harvest)
        db_session.commit()
        project_id = project.id

    return project_id


def file_len(file_name):
    if os.path.exists(file_name):
        with open(file_name) as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    return 0


def file_processor():
    while True:
        try:
            # project_info = queue.get()  # Read from the queue
            # project_info_split = project_info.split('%%_%%')
            # project_id = int(project_info_split[0])
            # if len(project_info_split) > 1:
            #     clustering_override = project_info_split[1]
            #     run_isi_workflow(int(project_id), clustering_override=clustering_override)
            # else:
            #     run_isi_workflow(int(project_info))

            # TODO: Figure out how to do the whole write pages business for ISI
            project_id = queue.get()
            run_isi_workflow(int(project_id), write_pages=True, get_debug_htmls=False)

            time.sleep(1)
        except:
            print '[%s] -- Error with project %d --' % \
                  (datetime.now().isoformat(), int(project_id))
            traceback.print_exc()
            logging.error('-- Error with project %d --' % (int(project_id)))
            logging.error(traceback.format_exc())
            Project.query.filter(Project.id == project_id).update({"status": ProjectStatus.error.value.lower()})
            db_session.commit()
            time.sleep(1)
