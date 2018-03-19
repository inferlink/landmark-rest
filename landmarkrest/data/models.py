from sqlalchemy import Column, Integer, BigInteger, String, Text, Enum, Boolean, UniqueConstraint, desc
from sqlalchemy.dialects.mysql import LONGTEXT, TINYINT
from sqlalchemy.orm.session import make_transient
from database import Base, db_session
from landmarkrest.data.views import ProjectView, ClusterView, HarvestView, HarvestViewBasic, PageView
from landmark_extractor.extraction.Landmark import RuleSet, IterationRule, ItemRule
from collections import OrderedDict
from settings import *
import enum
import json


class ProjectStatus(enum.Enum):
    new = 'New'
    harvesting = 'Harvesting'
    clustering = 'Clustering'
    learning = 'Learning'
    ready = 'Ready'
    error = 'Error'
    deleted = 'Deleted'
    published = 'Published'


class HarvestingStatus(enum.Enum):
    not_started = 'NOT_STARTED'
    running = 'RUNNING'
    completed_errors = 'COMPLETED_WITH_ERRORS'
    completed_success = 'COMPLETED'
    unable_to_start = 'UNABLE_TO_START'

    @classmethod
    def from_name(cls, name):
        for status_name, status_member in HarvestingStatus.__members__.items():
            if status_member.value == name:
                return status_member
        raise ValueError('{} is not a valid status name'.format(name))


class PageType(enum.Enum):
    train = 'train'
    test = 'test'
    other = 'other'

class Classifier(enum.Enum):
    default = 'default'
    rule = 'rule'

class Project(Base):
    __tablename__ = 'project'
    id = Column(Integer, primary_key=True)
    name = Column(String(175), unique=True)
    status = Column(Enum(ProjectStatus), nullable=False)
    selected_cluster_id = Column(Integer)

    def __init__(self, name, project_status=ProjectStatus.new):
        self.name = name
        self.status = project_status
        self.cluster_id = None

    def __repr__(self):
        return '<Project %r, %r, %r, Cluster %r>' % (self.id, self.name, self.status, self.selected_cluster_id)

    def get_view(self):
        return ProjectView(self.id, self.name, self.status.value.upper(), self.selected_cluster_id)

    def choose_cluster(self, selected_cluster_id):
        self.selected_cluster_id = selected_cluster_id

    def export_to_json(self):
        project_json = {
            "name": self.name.split('-_-')[-1],
            "status": self.status.value.upper(),
            "selected_cluster_id": self.selected_cluster_id
        }

        # now go get the crawl info
        db_harvest = Harvest.query.filter(Harvest.project_id == self.id).first()
        if db_harvest:
            harvest_json = db_harvest.export_to_json()
            project_json['harvest'] = harvest_json

        return project_json

    @staticmethod
    def import_from_json(project_json, new_isi_prefix='import'):
        project_name = new_isi_prefix + '-_-' + project_json['name']
        # if '-_-' in project_json['name']:
        #     project_name = project_json['name'].split('-_-')[-1]
        #     project_name = new_isi_prefix + '-_-' + project_name
        # else:
        #     project_name = project_name + '_import'

        project = Project(project_name, project_json['status'])
        project.selected_cluster_id = project_json['selected_cluster_id']
        db_session.add(project)
        db_session.commit()

        harvest_obj = project_json['harvest']
        harvest = Harvest(
            project_id=project.id,
            url=harvest_obj['url'],
            email=harvest_obj['email'],
            depth=harvest_obj['depth'],
            prefer_pagination=harvest_obj['prefer_pagination'],
            multi_urls=harvest_obj['multi_urls'],
            concurrent_requests=harvest_obj['concurrent_requests'],
            concurrent_requests_per_domain=harvest_obj['concurrent_requests_per_domain'],
            duration=harvest_obj['duration'],
            error_page_percentage=harvest_obj['error_page_percentage'],
            error_page_percentage_period=harvest_obj['error_page_percentage_period'],
            started_ms=harvest_obj['started_ms']
        )
        harvest.crawl_id = harvest_obj['crawl_id']
        harvest.status = HarvestingStatus.from_name(harvest_obj['status'].upper())
        harvest.pages_failed = harvest_obj['pages_failed']
        harvest.pages_fetched = harvest_obj['pages_fetched']
        harvest.jl_file_location = harvest_obj['jl_file_location']
        harvest.completed_ms = harvest_obj['completed_ms']

        db_session.add(harvest)
        db_session.commit()

        templates = list()

        clusters = harvest_obj['clusters']
        for cluster in clusters:
            db_cluster = Cluster(
                cluster_id=cluster['cluster_id'],
                project_id=project.id,
                harvest_id=harvest.id,
                anchor=cluster['anchor'],
                classifier=cluster['classifier']
            )
            db_cluster.dirty = cluster['dirty']
            db_session.add(db_cluster)
            db_session.commit()

            # now loop through the pages and add them
            clustered_pages = cluster['clustered_pages']
            for clustered_page in clustered_pages:
                # first add the page
                db_page = Page(
                    crawl_id=harvest.id,
                    crawl_page_id=clustered_page['page']['crawl_page_id'],
                    html=clustered_page['page']['html'],
                    live_url=clustered_page['page']['live_url'],
                    thumbnail_url=clustered_page['page']['thumbnail_url'],
                    small_thumbnail_url=clustered_page['page']['small_thumbnail_url']
                )
                db_session.add(db_page)
                db_session.commit()

                # then add the clustered_page
                db_clustered_page = ClusteredPage(
                    project_id=project.id,
                    cluster_id=db_cluster.cluster_id,
                    page_id=db_page.id,
                    page_type=clustered_page['page_type'],
                    alias=clustered_page['alias']
                )
                db_session.add(db_clustered_page)
                db_session.commit()

            # last thing to do is add the template ... then apply the extractions!
            for template in cluster['templates']:
                db_template = Template()
                db_template.stripes = template['stripes']
                db_session.add(db_template)
                db_session.flush()
                db_template.rules = template['rules']
                db_session.merge(db_template)
                db_session.flush()
                db_template.markup = template['markup']
                db_session.merge(db_template)
                db_session.flush()
                db_template.schema = template['schema']
                db_template.project_id = project.id
                db_template.cluster_id = db_cluster.cluster_id
                db_session.merge(db_template)
                db_session.flush()
                db_session.commit()
                templates.append(db_template)

        return project, templates

    @staticmethod
    def duplicate_from_db(project_id):
        project = Project.query.filter(Project.id == project_id).first()
        make_transient(project)
        project.id = None
        return project


class Harvest(Base):
    __tablename__ = 'harvest'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, nullable=False)
    crawl_id = Column(String(256))
    depth = Column(Integer)
    status = Column(Enum(HarvestingStatus), nullable=False)
    pages_failed = Column(Integer, default=0)
    pages_fetched = Column(Integer, default=0)
    jl_file_location = Column(Text)
    url = Column(String(512))
    email = Column(String(256))
    concurrent_requests = Column(Integer, default=0)
    concurrent_requests_per_domain = Column(Integer, default=0)
    duration = Column(Integer, default=0)
    error_page_percentage = Column(Integer, default=0)
    error_page_percentage_period = Column(Integer, default=0)
    started_ms = Column(BigInteger, default=0)
    completed_ms = Column(BigInteger)
    depth = Column(Integer, default=0)
    prefer_pagination = Column(TINYINT(1), default=0)
    multi_urls = Column(Text)

    def __init__(self, project_id, url, email, depth, prefer_pagination, multi_urls, concurrent_requests, 
        concurrent_requests_per_domain, duration, error_page_percentage, error_page_percentage_period, started_ms):
        self.project_id = project_id
        self.status = HarvestingStatus.not_started
        self.url = url
        self.email = email
        self.concurrent_requests = concurrent_requests
        self.concurrent_requests_per_domain = concurrent_requests_per_domain
        self.duration = duration
        self.error_page_percentage = error_page_percentage
        self.error_page_percentage_period = error_page_percentage_period
        self.started_ms = started_ms
        self.completed_ms = None
        self.depth = depth
        self.prefer_pagination = prefer_pagination
        self.multi_urls = multi_urls

    def get_view_basic(self):
        return HarvestViewBasic(self.project_id, self.id, self.status)

    def get_view(self):
        return HarvestView(self.project_id, self.id, self.status, self.pages_fetched, 
            self.pages_failed, self.jl_file_location, self.url, self.email, self.depth, self.prefer_pagination, self.multi_urls,
            self.concurrent_requests, self.concurrent_requests_per_domain, self.duration, self.error_page_percentage,
            self.error_page_percentage_period, self.started_ms, self.completed_ms)

    def export_to_json(self):
        json_obj = {
            "crawl_id": self.crawl_id,
            "depth": self.depth,
            "status": self.status.value,
            "pages_failed": self.pages_failed,
            "pages_fetched": self.pages_fetched,
            "jl_file_location": 'EXPORTED_BY_USER',
            "url": self.url,
            "email": self.email,
            "concurrent_requests": self.concurrent_requests,
            "concurrent_requests_per_domain": self.concurrent_requests_per_domain,
            "duration": self.duration,
            "error_page_percentage": self.error_page_percentage,
            "error_page_percentage_period": self.error_page_percentage_period,
            "started_ms": self.started_ms,
            "completed_ms": self.completed_ms,
            "prefer_pagination": self.prefer_pagination,
            "multi_urls": self.multi_urls
        }

        # add the cluster info
        db_clusters = Cluster.query.filter(Cluster.project_id == self.project_id)\
            .filter(Cluster.harvest_id == self.id).all()
        cluster_json = []
        for cluster in db_clusters:
            cluster_json.append(cluster.export_to_json())
        json_obj['clusters'] = cluster_json

        return json_obj


class Cluster(Base):
    __tablename__ = 'cluster'
    cluster_id = Column(Integer, nullable=False, primary_key=True)
    project_id = Column(Integer, nullable=False, primary_key=True)
    harvest_id = Column(Integer, nullable=False)
    dirty = Column(Boolean, default=False)
    anchor = Column(LONGTEXT)
    classifier = Column(Enum(Classifier), nullable=False, server_default=Classifier.default.value)

    def __init__(self, cluster_id, project_id=None, harvest_id=None, page_ids=list(), anchor=None, classifier=Classifier.default):
        self.cluster_id = cluster_id
        self.project_id = project_id
        self.harvest_id = harvest_id
        self.page_ids = page_ids
        self.dirty = False
        self.anchor = anchor
        self.classifier = classifier

    def __repr__(self):
        return '<Cluster %r, Pages: %r>' % (self.id, self.page_ids)

    def __str__(self):
        return 'id:' + str(self.cluster_id) + '; pageids:' + str(self.page_ids)

    def get_view(self, selected):
        return ClusterView(self.cluster_id, self.anchor, selected)

    def export_to_json(self):
        json_obj = {
            "cluster_id": self.cluster_id,
            "dirty": self.dirty,
            "anchor": self.anchor,
            "classifier": self.classifier.value
        }

        # append the clustered_page info
        db_clustered_pages = ClusteredPage.query.filter(ClusteredPage.project_id == self.project_id)\
            .filter(ClusteredPage.cluster_id == self.cluster_id).all()
        clustered_pages_json = []
        for clustered_page in db_clustered_pages:
            clustered_pages_json.append(clustered_page.export_to_json())
        json_obj['clustered_pages'] = clustered_pages_json

        # append the template info
        db_templates = Template.query.filter(Template.project_id == self.project_id)\
            .filter(Template.cluster_id == self.cluster_id).all()
        templates_json = []
        for template in db_templates:
            templates_json.append(template.export_to_json())
        json_obj['templates'] = templates_json

        # skip the debug_templates for now... too much info

        return json_obj


class Page(Base):
    __tablename__ = 'page'
    id = Column(Integer, primary_key=True)
    crawl_id = Column(Integer, nullable=False)
    crawl_page_id = Column(String(80), nullable=False)
    html = Column(LONGTEXT)
    live_url = Column(Text)
    small_thumbnail_url = Column(Text)
    thumbnail_url = Column(Text)

    __table_args__ = (UniqueConstraint('crawl_id', 'crawl_page_id', name='cid_cpid_uix_1'),
                      )

    def __init__(self, crawl_id, crawl_page_id, html, live_url, thumbnail_url, small_thumbnail_url=None):
        self.crawl_id = crawl_id
        self.crawl_page_id = crawl_page_id
        self.html = html
        self.live_url = live_url
        self.thumbnail_url = thumbnail_url
        self.small_thumbnail_url = small_thumbnail_url

    def export_to_json(self):
        json_obj = {
            "crawl_page_id": self.crawl_page_id,
            "html": self.html,
            "live_url": self.live_url,
            "small_thumbnail_url": self.small_thumbnail_url,
            "thumbnail_url": self.thumbnail_url
        }
        return json_obj


class ClusteredPage(Base):
    __tablename__ = 'clustered_page'
    project_id = Column(Integer, primary_key=True)
    cluster_id = Column(Integer, primary_key=True)
    page_id = Column(Integer, primary_key=True)
    page_type = Column(Enum(PageType), nullable=False)
    alias = Column(String(255))

    def __init__(self, project_id, page_id, cluster_id, page_type, alias):
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.page_id = page_id
        self.page_type = page_type
        self.alias = alias

    def __repr__(self):
        return '<ClusteredPage projid: %r, cid: %r, pid: %r, type: %r>' %\
               (self.project_id, self.cluster_id, self.page_id, self.page_type)

    def set_page_type(self, page_type):
        self.page_type = page_type

    def set_cluster_id(self, cluster_id):
        self.cluster_id = cluster_id

    def set_alias(self, alias):
        self.alias = alias

    def get_name(self):
        name = self.alias
        if not name:
            name = 'page'+str(self.page_id)
        return name

    def get_view(self):
        real_page = Page.query.filter(Page.id == self.page_id).first()
        return PageView(self.page_id,
                        self.get_name(),
                        real_page.live_url,
                        real_page.thumbnail_url,
                        real_page.small_thumbnail_url,
                        self.page_type.value.lower())

    def export_to_json(self):
        json_obj = {
            "page_type": self.page_type.value,
            "alias": self.alias
        }

        # append the page info
        db_page = Page.query.filter(Page.id == self.page_id).first()
        if db_page:
            json_obj['page'] = db_page.export_to_json()

        return json_obj


class ValidationTable(Base):
    __tablename__ = 'validation'
    project_id = Column(Integer, primary_key=True)
    cluster_id = Column(Integer, primary_key=True)
    field_id = Column(String(64), primary_key=True)
    validation_type = Column(String(32), primary_key=True)
    validation_param = Column(String(255), nullable=True)
    
    def __init__(self, project_id, cluster_id, field_id, validation_type, validation_param = None):
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.field_id = field_id
        self.validation_type = validation_type
        self.validation_param = validation_param
    
    @staticmethod
    def get_schema_validations(project_id, cluster_id):
        validations_for_schema = ValidationTable.query.filter(ValidationTable.project_id == project_id). \
            filter(ValidationTable.cluster_id == cluster_id).order_by(desc(ValidationTable.field_id))
        return validations_for_schema


def get_sub_schema(markup):
    """Helper function to get sub schema"""
    fields = []
    for name in markup:
        markup_object = markup[name]

        sub_field = {'field': name, 'schemaid': markup_object['rule_id']}
        if 'sequence' in markup_object:
            sub_field['list'] = []
            if len(markup_object['sequence']) > 0 and 'sub_rules' in markup_object['sequence'][0]:
                sub_field['list'] = get_sub_schema(markup_object['sequence'][0]['sub_rules'])

        fields.append(sub_field)
    return fields


# def get_flattened_sub_schema(fields, parent_name=''):
#     """Helper function to get flattened sub schema from markup"""
#     flattened_fields = []
#     for field in fields:
#         flattened_field = dict()
#
#         if 'list' in field and field['list']:
#             flattened_fields.extend(get_flattened_sub_schema(field['list'], field['field']+'__'))
#         else:
#             flattened_field['name'] = parent_name + field['field']
#             flattened_field['schemaid'] = field['schemaid']
#
#             flattened_fields.append(flattened_field)
#     return flattened_fields

def get_flattened_sub_schema(schema_fields, parent_name=''):
    """Helper function to get flattened sub schema from schema"""
    flattened_fields = []
    for field in schema_fields:
        flattened_field = dict()

        if 'list' in field and field['list']:
            flattened_fields.extend(get_flattened_sub_schema(field['list'], field['field']+'__'))
        else:
            flattened_field['name'] = parent_name + field['field']
            flattened_field['schemaid'] = field['schemaid']
            if 'mapped' in field:
                flattened_field['mapped'] = field['mapped']

            flattened_fields.append(flattened_field)
    return flattened_fields


def update_rule(rules, field_id, new_name=None):
    """Helper function to find and rename or delete rule"""
    updated_rule_id = None
    rule_names = []
    rule_to_update = None

    for rule in rules:
        if rule['id'] == field_id:
            if new_name:
                rule_to_update = rule
                break
            else:
                rules.remove(rule)
                return field_id
        elif 'sub_rules' in rule:
            return update_rule(rule['sub_rules'], field_id, new_name)

    for rule in rules:
        rule_names.append(rule['name'])

    if rule_to_update:
        if new_name not in rule_names or new_name == rule_to_update['name']:
            rule_to_update['name'] = new_name
            updated_rule_id = rule_to_update['id']

    return updated_rule_id


def update_markup(markup, field_id, new_name=None):
    """Helper function to find and rename or delete value from markup"""
    if isinstance(markup, list):
        # this is a sequence item so we look at the 'sub_rules' if they exist
        for row in markup:
            if 'sub_rules' in row:
                return update_markup(row['sub_rules'], field_id, new_name)
    elif isinstance(markup, dict):
        # this is a standard rule so we look at this one itself
        siblings = []
        update_name = None
        update_id = None
        for name in markup:
            if 'rule_id' in markup[name] and markup[name]['rule_id'] == field_id:
                update_name = name
            elif 'sub_rules' in markup[name]:
                update_id = update_markup(markup[name]['sub_rules'], field_id, new_name)
                if update_id:
                    return update_id
            elif 'sequence' in markup[name]:
                update_id = update_markup(markup[name]['sequence'], field_id, new_name)
                if update_id:
                    return update_id
            else:
                siblings.append(name)

        if update_name:
            if new_name:
                if new_name not in siblings:
                    update_id = field_id
            else:
                markup.pop(update_name)
                update_id = field_id

        return update_id


def update_schema(schema, field_id, new_name=None):
    """Helper function to find and rename or delete schema"""
    field_names = []
    schema_update = None
    update_id = None
    for schema_item in schema:
        if schema_item['schemaid'] == field_id:
            schema_update = schema_item
        elif 'list' in schema_item:
            update_id = update_schema(schema_item['list'], field_id, new_name)
            if update_id:
                return update_id
        else:
            field_names.append(schema_item['field'])

    if schema_update:
        if new_name:
            if new_name not in field_names:
                schema_update['field'] = new_name
                schema_update['mapped'] = True
                update_id = field_id
        else:
            schema.remove(schema_update)
            update_id = field_id

    return update_id


def add_field_to_schema(schema, name, rule_type, parent_id):
    new_field_id = None
    field_names = []
    if parent_id and 'list' in schema:
        for field in schema['list']:
            if field['schemaid'] == parent_id:
                return add_field_to_schema(field, name, rule_type, parent_id=None)
    else:
        if 'list' in schema:
            for field in schema['list']:
                field_names.append(field['field'])
        else:
            schema['list'] = []

        if name not in field_names:
            new_field = {'field': name, 'schemaid': str(uuid.uuid4())}
            if rule_type == 'list':
                new_field['list'] = []

            schema['list'].insert(0, new_field)
            new_field_id = new_field['schemaid']

    return new_field_id


# def add_rule(rules, name, rule_type, parent_id):
#     """Helper function to add rule"""
#     new_rule = None
#     rule_names = []
#     if parent_id:
#         for rule in rules:
#             if rule['id'] == parent_id:
#                 return add_rule(rule, name, rule_type, parent_id=None)
#             elif 'sub_rules' in rule:
#                 return add_rule(rule['sub_rules'], name, rule_type, parent_id)
#     else:
#         if isinstance(rules, list):
#             for rule in rules:
#                 rule_names.append(rule['name'])
#         elif isinstance(rules, dict):
#             if 'sub_rules' in rules:
#                 for rule in rules['sub_rules']:
#                     rule_names.append(rule['name'])
#
#         if name not in rule_names:
#             if rule_type == 'item':
#                 new_rule = ItemRule(name, 'USER_ADDED', 'USER_ADDED')
#             elif rule_type == 'list':
#                 new_rule = IterationRule(name, 'USER_ADDED', 'USER_ADDED', 'USER_ADDED', 'USER_ADDED')
#
#     new_rule_id = None
#     if new_rule:
#         rule = json.loads(new_rule.toJson())
#
#         if isinstance(rules, list):
#             rules.append(rule)
#         elif isinstance(rules, dict):
#             if 'sub_rules' not in rules:
#                 rules['sub_rules'] = list()
#             rules['sub_rules'].insert(0, rule)
#
#         new_rule_id = rule['id']
#     return new_rule_id


class Template(Base):
    __tablename__ = 'template'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer)
    cluster_id = Column(Integer)
    supervised = Column(Boolean)
    markup = Column(LONGTEXT)
    stripes = Column(LONGTEXT)
    rules = Column(LONGTEXT)
    schema = Column(LONGTEXT)

    __table_args__ = (UniqueConstraint('project_id', 'cluster_id', 'supervised', name='pid_cid_sup_uix_1'),
                      )

    @staticmethod
    def duplicate_from_db(project_id, cluster_id):
        template = Template.query.filter(Template.project_id == project_id). \
            filter(Template.cluster_id == cluster_id).order_by(desc(Template.supervised)).first()
        make_transient(template)
        template.id = None
        return template

    @staticmethod
    def get_from_db(project_id, cluster_id):
        return Template.query.filter(Template.project_id == project_id). \
            filter(Template.cluster_id == cluster_id).order_by(desc(Template.supervised)).first()

    def __init__(self, stripes=None, pages=None, rules=None, supervised=False, markup=None):
        self.stripes = stripes
        self.pages = pages
        self.rules = rules
        self.supervised = supervised
        self.markup = markup

        if self.markup:
            self.set_schema_view()

    def __repr__(self):
        return '<Template %r, Project id: %r, Cluster id: %r>' % (self.id, self.project_id, self.cluster_id)

    def set_schema_view(self):
        root = dict()
        root['field'] = '[root]'
        root['schemaid'] = '0'

        markup_object = json.loads(self.markup, object_pairs_hook=OrderedDict)
        # TODO: Figure out how to loop through and just update the structure
        first_markup = markup_object.itervalues().next()
        sub_fields = get_sub_schema(first_markup)
        root['list'] = sub_fields

        fields = list()
        fields.append(root)
        self.schema = json.dumps(fields, indent=2)

    def get_flattened_schema_view(self):
        # BA: 20171018 - Updating this to use the schema. Legacy stuff used markup which I don't think we should do.
        schema_object = json.loads(self.schema, object_pairs_hook=OrderedDict)
        flattened_schema = get_flattened_sub_schema(schema_object[0]['list'])
        return flattened_schema

        # markup_object = json.loads(self.markup, object_pairs_hook=OrderedDict)
        # # TODO: Figure out how to loop through and just update the structure
        # first_markup = markup_object.itervalues().next()
        # sub_fields = get_sub_schema(first_markup)
        # flattened_schema = get_flattened_sub_schema(sub_fields)
        # return flattened_schema

    def update_template(self, stripes=None, rules=None, markup=None):
        self.stripes = json.dumps(stripes, indent=2)
        self.rules = json.dumps(rules, indent=2)
        self.markup = json.dumps(markup, indent=2)
        # We don't update the schema, that should already be all set

    def apply_template(self, page, rule_ids_to_run=None):
        json_object = json.loads(self.rules, object_pairs_hook=OrderedDict)
        rules = RuleSet(json_object)

        if rule_ids_to_run:
            updated_rules = RuleSet()
            for rule in rules.rules:
                if rule.id in rule_ids_to_run:
                    updated_rules.add_rule(rule)
            rules = updated_rules

        json_extractions = rules.extract(page.html)
        return json_extractions

    def set_markup_object(self, page_id, page_markup_object):
        page_id = str(page_id)
        markup_object = json.loads(self.markup, object_pairs_hook=OrderedDict)
        if page_markup_object:
            markup_object[page_id] = page_markup_object
        else:
            if page_id in markup_object:
                del markup_object[page_id]
        self.markup = json.dumps(markup_object, indent=2)

    def update_markup_from_extraction_view(self, page_id, extraction_view):
        page_id = str(page_id)
        markup_object = json.loads(self.markup, object_pairs_hook=OrderedDict)
        markup_object[page_id] = revert_extraction_view(extraction_view)
        self.markup = json.dumps(markup_object, indent=2)
        return markup_object[page_id]

    def update_field(self, field_id, new_name=None):

        # update the rules object
        rules_object = json.loads(self.rules, object_pairs_hook=OrderedDict)
        rule_id = update_rule(rules_object, field_id, new_name)
        updated = False
        if rule_id:
            self.rules = json.dumps(rules_object, indent=2)
            updated = True

        # update the markup object
        markup_object = json.loads(self.markup, object_pairs_hook=OrderedDict)
        markup_ids = set()
        for page_id in markup_object:
            markup_id = update_markup(markup_object[page_id], field_id, new_name)
            if markup_id:
                if new_name:
                    markup_object[page_id] = OrderedDict([(new_name, v) if v['rule_id'] == markup_id
                                                          else (k, v) for k, v in markup_object[page_id].items()])
                markup_ids.add(markup_id)
        if len(markup_ids) > 0:
            self.markup = json.dumps(markup_object, indent=2)
            updated = True

        # update the schema object
        schema_object = json.loads(self.schema, object_pairs_hook=OrderedDict)
        schema_id = update_schema(schema_object[0]['list'], field_id, new_name)
        if schema_id:
            self.schema = json.dumps(schema_object, indent=2)
            updated = True

        if updated:
            return field_id
        return None

    def add_field(self, name, rule_type, parent_id=None):
        schema = json.loads(self.schema, object_pairs_hook=OrderedDict)
        new_rule_id = add_field_to_schema(schema[0], name, rule_type, parent_id)

        if new_rule_id:
            self.schema = json.dumps(schema, indent=2)
            return new_rule_id
        return None

    def export_to_json(self):
        json_obj = {
            "supervised": self.supervised,
            "markup": self.markup,
            "stripes": self.stripes,
            "rules": self.rules,
            "schema": self.schema
        }

        return json_obj


class TemplateDebug(Base):
    __tablename__ = 'template_debug'
    project_id = Column(Integer, primary_key=True)
    cluster_id = Column(Integer, primary_key=True)
    template_id = Column(Integer, primary_key=True)
    page_id = Column(Integer, primary_key=True)
    debug_html = Column(LONGTEXT)

    def __init__(self, project_id, cluster_id, template_id, page_id, debug_html):
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.template_id = template_id
        self.page_id = page_id
        self.debug_html = debug_html


def get_extraction_view(extraction, markup, schema):
    extraction_view = []
    for schema_object in schema:
        schema_id = schema_object['schemaid']
        extraction_object = None
        if extraction:
            for name in extraction:
                if extraction[name]['rule_id'] == schema_id:
                    extraction_object = extraction[name]
                    break

        markup_object = None
        if markup:
            for name in markup:
                if markup[name]['rule_id'] == schema_id:
                    markup_object = markup[name]
                    break

        page_value = get_extraction_view_for_item(extraction_object, markup_object, schema_object)
        extraction_view.append(page_value)

    return extraction_view


def get_extraction_view_for_item(extraction_object, markup_object, schema_object):

    name = schema_object['field']
    schema_id = schema_object['schemaid']

    page_value = dict()
    page_value['Name'] = name
    page_value['schemaid'] = schema_id

    if markup_object and 'extract' in markup_object:
        page_value['Markup'] = markup_object['extract']
    else:
        page_value['Markup'] = ""

    if extraction_object and 'extract' in extraction_object:
        page_value['Extraction'] = extraction_object['extract']
    else:
        page_value['Extraction'] = ""

    if 'list' in schema_object:
        page_value['type'] = 'list'
        page_value['children'] = []

        list_data = {}

        if extraction_object and 'sequence' in extraction_object:
            extraction_list_object = extraction_object['sequence']

            for extraction_item in extraction_list_object:
                sequence_number = extraction_item['sequence_number']
                list_data[sequence_number] = {}
                list_data[sequence_number]['extraction_item'] = extraction_item

        if markup_object and 'sequence' in markup_object:
            markup_list_object = markup_object['sequence']

            for markup_item in markup_list_object:
                sequence_number = markup_item['sequence_number']
                if sequence_number not in list_data:
                    list_data[sequence_number] = {}
                list_data[sequence_number]['markup_item'] = markup_item

        list_data_ordered = OrderedDict(sorted(list_data.items()))
        for k, v in list_data_ordered.iteritems():
            row_page_value = dict()
            row_page_value['type'] = 'row'
            row_page_value['Name'] = str(k)

            extraction = None
            row_page_value['Extraction'] = ''
            if 'extraction_item' in v:
                extraction_item = v['extraction_item']
                row_page_value['Extraction'] = extraction_item['extract']
                if 'sub_rules' in extraction_item:
                    extraction = extraction_item['sub_rules']

            markup = None
            row_page_value['Markup'] = ''
            if 'markup_item' in v:
                markup_item = v['markup_item']
                row_page_value['Markup'] = markup_item['extract']
                if 'sub_rules' in markup_item:
                    markup = markup_item['sub_rules']

            row_page_value['children'] = get_extraction_view(extraction, markup, schema_object['list'])

            page_value['children'].append(row_page_value)

    else:
        page_value['type'] = 'item'

    return page_value


def get_extraction_view_with_validation(extraction, markup, schema):
    extraction_view = []
    for schema_object in schema:
        schema_id = schema_object['schemaid']
        extraction_object = None
        if extraction:
            for name in extraction:
                if extraction[name]['rule_id'] == schema_id:
                    extraction_object = extraction[name]
                    break

        markup_object = None
        if markup:
            for name in markup:
                if markup[name]['rule_id'] == schema_id:
                    markup_object = markup[name]
                    break

        page_value = get_extraction_view_for_item_with_validation(extraction_object, markup_object, schema_object)
        extraction_view.append(page_value)

    return extraction_view


def get_extraction_view_for_item_with_validation(extraction_object, markup_object, schema_object):

    name = schema_object['field']
    schema_id = schema_object['schemaid']

    page_value = dict()
    page_value['Name'] = name
    page_value['schemaid'] = schema_id

    if markup_object and 'extract' in markup_object:
        page_value['Markup'] = markup_object['extract']
    else:
        page_value['Markup'] = ""

    if extraction_object and 'extract' in extraction_object:
        page_value['Extraction'] = extraction_object['extract']
    else:
        page_value['Extraction'] = ""

    if extraction_object and 'valid' in extraction_object:
        page_value['valid'] = extraction_object['valid']
    if extraction_object and 'validation' in extraction_object:
        page_value['validation'] = extraction_object['validation']

    if 'list' in schema_object:
        page_value['type'] = 'list'
        page_value['children'] = []

        list_data = {}

        if extraction_object and 'sequence' in extraction_object:
            extraction_list_object = extraction_object['sequence']

            for extraction_item in extraction_list_object:
                sequence_number = extraction_item['sequence_number']
                list_data[sequence_number] = {}
                list_data[sequence_number]['extraction_item'] = extraction_item

        if markup_object and 'sequence' in markup_object:
            markup_list_object = markup_object['sequence']

            for markup_item in markup_list_object:
                sequence_number = markup_item['sequence_number']
                if sequence_number not in list_data:
                    list_data[sequence_number] = {}
                list_data[sequence_number]['markup_item'] = markup_item

        list_data_ordered = OrderedDict(sorted(list_data.items()))
        for k, v in list_data_ordered.iteritems():
            row_page_value = dict()
            row_page_value['type'] = 'row'
            row_page_value['Name'] = str(k)

            extraction = None
            row_page_value['Extraction'] = ''
            if 'extraction_item' in v:
                extraction_item = v['extraction_item']
                row_page_value['Extraction'] = extraction_item['extract']
                if 'sub_rules' in extraction_item:
                    extraction = extraction_item['sub_rules']

            markup = None
            row_page_value['Markup'] = ''
            if 'markup_item' in v:
                markup_item = v['markup_item']
                row_page_value['Markup'] = markup_item['extract']
                if 'sub_rules' in markup_item:
                    markup = markup_item['sub_rules']

            row_page_value['children'] = get_extraction_view_with_validation(extraction, markup, schema_object['list'])

            page_value['children'].append(row_page_value)

    else:
        page_value['type'] = 'item'

    return page_value


def revert_extraction_view(markup_object_list):
    reverted_markup = dict()
    for markup_object in markup_object_list:
        markup_item = revert_extraction_view_item(markup_object)
        if markup_item:
            reverted_markup[markup_object['Name']] = markup_item
    return reverted_markup


def revert_extraction_view_item(markup_object):
    markup_item = dict()
    if 'Markup' in markup_object:
        markup_item['extract'] = markup_object['Markup']
        markup_item['rule_id'] = markup_object['schemaid']

    if markup_object['type'] == 'list' and markup_object['children']:
        markup_item['sequence'] = revert_extraction_view_list(markup_object['children'])

    return markup_item


def revert_extraction_view_list(markup_list_object):
    reverted_markup = list()
    for list_item in markup_list_object:
        if 'Markup' in list_item:
            markup_list_item = dict()
            markup_list_item['extract'] = list_item['Markup']

        if list_item['type'] == 'row':
            markup_list_item['sequence_number'] = int(list_item['Name'])
        if list_item['children']:
            markup_list_item['sub_rules'] = revert_extraction_view(list_item['children'])

        reverted_markup.append(markup_list_item)

    return reverted_markup


def get_extraction_values(extraction_object, rule_id):
    values = list()

    if isinstance(extraction_object, list):
        for row in extraction_object:
            if 'sub_rules' in row:
                values.extend(get_extraction_values(row['sub_rules'], rule_id))

    elif isinstance(extraction_object, dict):
        for name in extraction_object:
            if extraction_object[name]['rule_id'] == rule_id:
                if 'sequence' in extraction_object[name]:
                    # then add the "row" row items
                    for item in extraction_object[name]['sequence']:
                        values.append({'value': item['extract']})
                else:
                    values.append({'value': extraction_object[name]['extract']})
            elif 'sequence' in extraction_object[name]:
                values.extend(get_extraction_values(extraction_object[name]['sequence'], rule_id))

    return values


def get_extraction_values_with_validation(extraction_object, rule_id):
    values = list()
    if isinstance(extraction_object, list):
        for row in extraction_object:
            if 'sub_rules' in row:
                values.extend(get_extraction_values_with_validation(row['sub_rules'], rule_id))
    elif isinstance(extraction_object, dict):
        for name in extraction_object:
            if extraction_object[name]['rule_id'] == rule_id:
                if 'sequence' in extraction_object[name]:
                    # then add the "row" row items
                    for item in extraction_object[name]['sequence']:
                        if 'valid' in item:
                            values.append({'value': item['extract'], 'valid': item['valid']})
                        else:
                            values.append({'value': item['extract']})

                else:
                    if 'valid' in extraction_object[name]:
                        values.append({'value': extraction_object[name]['extract'], 'valid': extraction_object[name]['valid']})
                    else:
                        values.append({'value': extraction_object[name]['extract']})
            elif 'sequence' in extraction_object[name]:
                values.extend(get_extraction_values_with_validation(extraction_object[name]['sequence'], rule_id))
    return values


def get_extraction_values_with_validation_add_list_validation(extraction_object, rule_id, rule_name):
    # get list validations that we will append to fields belonging to the list
    list_validations = {}
    for name in extraction_object:
        if 'sequence' in extraction_object[name] and 'validation' in extraction_object[name]:
            # this is a list with validation
            list_validations[name] = extraction_object[name]['validation']
    values = get_extraction_values_with_validation(extraction_object, rule_id)
    # add list validations to values IF values belong to a list with validation
    #use list name (we don't have list schemaid info at this level
    for list_name in list_validations:
        if rule_name.startswith(list_name):
            for validation in list_validations[list_name]:
                if validation['valid'] is False:
                    if 'param' in validation:
                        values.insert(0, {'value': validation['type']+':'+validation['param'], 'valid': validation['valid']})
                    else:
                        values.insert(0, {'value': validation['type'], 'valid': validation['valid']})
    return values


class Extraction(Base):
    __tablename__ = 'extraction'
    project_id = Column(Integer, primary_key=True)
    cluster_id = Column(Integer, primary_key=True)
    template_id = Column(Integer, primary_key=True)
    page_id = Column(Integer, primary_key=True)
    extraction_json = Column(LONGTEXT)
    extraction_json_with_validation = Column(LONGTEXT)
    extraction_object_with_validation = None

    def __init__(self, project_id, cluster_id, template_id, page_id, extraction_json):
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.template_id = template_id
        self.page_id = page_id
        self.extraction_json = extraction_json

    def get_values(self, rule_id):
        extraction_object = json.loads(self.extraction_json, object_pairs_hook=OrderedDict)
        return get_extraction_values(extraction_object, rule_id)

    def get_fix_view(self, markup_object, schema_object):
        return get_extraction_view(json.loads(self.extraction_json, object_pairs_hook=OrderedDict),
                                   markup_object, schema_object)

    def get_fix_view_with_validation(self, markup_object, schema_object):
        return get_extraction_view_with_validation(json.loads(self.extraction_json_with_validation, object_pairs_hook=OrderedDict),
                                   markup_object, schema_object)

    def get_values_with_validation(self, rule_id, rule_name):
        if self.extraction_object_with_validation is None:
            self.extraction_object_with_validation = json.loads(self.extraction_json_with_validation, object_pairs_hook=OrderedDict)
        return get_extraction_values_with_validation_add_list_validation(self.extraction_object_with_validation, rule_id, rule_name)

    def update_extraction_with_validation(self, extraction_json_with_validation):
        self.extraction_json_with_validation = extraction_json_with_validation

    def export_to_json(self):
        json_obj = {
            "extraction_json": self.extraction_json,
            "extraction_json_with_validation": self.extraction_json_with_validation
        }

        return json_obj
