import json
from flask.json import JSONEncoder
from landmarkrest.data.models import Cluster, Template, Harvest
from landmarkrest.data.views import ClusterView, ProjectView, HarvestView, HarvestViewBasic, PageView
from collections import OrderedDict

class LandmarkJSONEncoder(JSONEncoder):
    """
    LandmarkJSONEncoder for Objects that are not Serializable
    """

    def default(self, obj):
        if isinstance(obj, Template):
            stripes = obj.stripes
            if isinstance(stripes, basestring):
                stripes = json.loads(stripes, object_pairs_hook=OrderedDict)

            rules = obj.rules
            if isinstance(rules, basestring):
                rules = json.loads(rules, object_pairs_hook=OrderedDict)

            markup = obj.markup
            if isinstance(markup, basestring):
                markup = json.loads(markup, object_pairs_hook=OrderedDict)

            return {
                'stripes': stripes,
                'rules': rules,
                'markup': markup,
                'supervised': obj.supervised,
            }
        elif isinstance(obj, ProjectView):
            return {
                'id': obj.id,
                'name': obj.name,
                'status': obj.status,
                'selected_cluster_id': obj.selected_cluster_id,
            }
        elif isinstance(obj, Cluster):
            return {
                'id': obj.cluster_id,
                'page_ids': obj.page_ids,
                'anchor': obj.anchor
            }
        elif isinstance(obj, HarvestView):
            return {
                'project_id': obj.project_id,
                'crawl_id': obj.crawl_id,
                'status': obj.status,
                'pages_fetched': obj.pages_fetched,
                'pages_failed': obj.pages_failed,
                'jl_file_location': obj.jl_file_location,
                'url': obj.url,
                'multi_urls': obj.multi_urls,
                'email': obj.email,
                'depth': obj.depth,
                'prefer_pagination': obj.prefer_pagination,
                'concurrent_requests': obj.concurrent_requests,
                'concurrent_requests_per_domain': obj.concurrent_requests_per_domain,
                'duration': obj.duration,
                'error_page_percentage': obj.error_page_percentage,
                'error_page_percentage_period': obj.error_page_percentage_period,
                'started_ms': obj.started_ms,
                'completed_ms': obj.completed_ms,
            }
        elif isinstance(obj, HarvestViewBasic):
            return {
                'project_id': obj.project_id,
                'crawl_id': obj.crawl_id,
                'status': obj.status,
            }
        elif isinstance(obj, ClusterView):
            return {
                'id': obj.id,
                'anchor': obj.anchor,
                'selected': obj.selected,
                'train': obj.train_pages,
                'train_pages': len(obj.train_pages),
                'test': obj.test_pages,
                'test_pages': len(obj.test_pages),
                'other': obj.other_pages,
                'other_pages': len(obj.other_pages),
            }
        elif isinstance(obj, PageView):
            return_obj = {
                'page_id': obj.page_id,
                'file': obj.file,
                'live_url': obj.live_url,
                'thumbnail_url': obj.thumbnail_url,
                'small_thumbnail_url': obj.small_thumbnail_url,
                'type': obj.type,
            }
            if obj.fields:
                return_obj['fields'] = obj.fields
            if obj.cluster_id:
                return_obj['cluster_id'] = obj.cluster_id
            if obj.valid is not None:
                return_obj['valid'] = obj.valid

            return return_obj

        return super(LandmarkJSONEncoder, self).default(obj)
