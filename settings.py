import uuid
import os

MODE = os.getenv('MODE', '')

# Used to override when working with images locally
SERVER_URL = ''

DB_CONNECTION = os.getenv('DB_CONNECTION', 'mysql://root:@localhost/sse?charset=utf8mb4')

CLUSTERING_URL = os.getenv('CLUSTERING_URL', 'http://localhost:5000/clustering')
LEARNING_URL = os.getenv('LEARNING_URL', 'http://localhost:5000/learning')

LANDMARK_URL = os.getenv('LANDMARK_URL', 'http://localhost:5000/')
S3_CRAWL_BUCKET = os.getenv('S3_CRAWL_BUCKET', 'http://inferlink-landmark-test.s3-website-us-east-1.amazonaws.com/')
LOCAL_S3_DIR = os.getenv('LOCAL_S3_DIR', 's3-repo/')

CRAWLER_ENDPOINT = os.getenv('CRAWLER_ENDPOINT', 'http://54.172.237.102:8080')

IMAGE_GEN_ENDPOINT = ''

HARVEST_PREFIX = 'h-'+uuid.uuid4().hex+'-'
URL_HARVEST_PREFIX = 'u-'+uuid.uuid4().hex+'-'

EMAIL_USER = os.getenv('EMAIL_USER', 'landmarktool@gmail.com')
EMAIL_PASS = os.getenv('EMAIL_PASS', 'KesVdQgtPF5f')
EMAIL_ACCOUNT = {'username': EMAIL_USER, 'password': EMAIL_PASS}

UNSUPERVISED_LEARN_LISTS = os.getenv('UNSUPERVISED_LEARN_LISTS', False)
CLUSTERING_ALGORITHM = os.getenv('CLUSTERING_ALGORITHM', 'URL')
CLUSTER_MERGING = False

### FOR ISI ###
TOP_SITES = 10
PAGES_PER_SITE = os.getenv('PAGES_PER_SITE', 200)
ISI_HARVEST_PREFIX = 'landmark_'+uuid.uuid4().hex+'-'
ISI_FIELDS_ENDPOINT = os.getenv('ISI_FIELDS_ENDPOINT', '')
FIELDS_ENDPOINT_AUTH_TOKEN = os.getenv('FIELDS_ENDPOINT_AUTH_TOKEN', '')

### FOR KAFKA ###
KAFKA_CONNECTIONS = os.getenv('KAFKA_CONNECTIONS', 'localhost:9092')
KAFKA_CONSUME_TOPICS = os.getenv('KAFKA_CONSUME_TOPICS', 'landmark')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'landmark')
KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL', '')
KAFKA_SSL_CAFILE = os.getenv('KAFKA_SSL_CAFILE', '')
KAFKA_SSL_CERTFILE = os.getenv('KAFKA_SSL_CERTFILE', '')
KAFKA_SSL_KEYFILE = os.getenv('KAFKA_SSL_KEYFILE', '')
