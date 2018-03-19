
class ProjectView(object):

    def __init__(self, project_id, name, status, selected_cluster_id):
        self.id = project_id
        self.name = name
        self.status = status
        self.selected_cluster_id = selected_cluster_id


class ClusterView(object):

    def __init__(self, cluster_id, anchor, selected=False):
        self.id = cluster_id
        self.anchor = anchor
        self.train_pages = list()
        self.test_pages = list()
        self.other_pages = list()
        self.selected = selected

    def add_page(self, page_id, page_type, file_name, live_url, thumbnail_url, small_thumbnail_url):
        page = dict()
        page['page_id'] = page_id
        page['file'] = file_name
        page['live_url'] = live_url
        page['thumbnail_url'] = thumbnail_url
        page['small_thumbnail_url'] = small_thumbnail_url

        if page_type == 'train':
            self.train_pages.append(page)
        elif page_type == 'test':
            self.test_pages.append(page)
        elif page_type == 'other':
            self.other_pages.append(page)


class PageView(object):

    def __init__(self, page_id, page_file, live_url, thumbnail_url, small_thumbnail_url, page_type):
        self.page_id = page_id
        self.file = page_file
        self.live_url = live_url
        self.thumbnail_url = thumbnail_url
        self.small_thumbnail_url = small_thumbnail_url
        self.type = page_type
        self.fields = dict()
        self.cluster_id = None
        self.valid = None

    def add_field_values(self, rule_name, field_values):
        self.fields[rule_name] = field_values


class HarvestView(object):

    def __init__(self, project_id, crawl_id, status, pages_fetched, pages_failed, jl_url, url, email, 
        depth, prefer_pagination, multi_urls, concurrent_requests, concurrent_requests_per_domain, duration, 
        error_page_percentage, error_page_percentage_period, started_ms, completed_ms):
        self.project_id = project_id
        self.crawl_id = crawl_id
        self.status = status.value
        self.pages_fetched = pages_fetched
        self.pages_failed = pages_failed
        self.jl_file_location = jl_url
        self.url = url
        self.email = email
        self.depth = depth
        self.prefer_pagination = prefer_pagination
        self.multi_urls = multi_urls
        self.concurrent_requests = concurrent_requests
        self.concurrent_requests_per_domain = concurrent_requests_per_domain
        self.duration = duration
        self.error_page_percentage = error_page_percentage
        self.error_page_percentage_period = error_page_percentage_period
        self.started_ms = started_ms
        self.completed_ms = completed_ms


class HarvestViewBasic(object):

    def __init__(self, project_id, crawl_id, status):
        self.project_id = project_id
        self.crawl_id = crawl_id
        self.status = status.value
