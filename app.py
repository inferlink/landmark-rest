import json
import logging.handlers
import os
import sys

from flask import Flask

from database import db_session
from landmarkrest.clustering import clustering_api
from landmarkrest.extraction import extraction_api
from landmarkrest.harvesting import harvesting_api
from landmarkrest.learning import learning_api
from landmarkrest.project import project_api, file_processor, queue
from landmarkrest.data.encoder import LandmarkJSONEncoder
from settings import *
import multiprocessing
from flask_compress import Compress
from middleware import gzip_http_request_middleware

application = Flask(__name__)
Compress(application)
application.before_request(gzip_http_request_middleware)
application.json_encoder = LandmarkJSONEncoder

application.register_blueprint(harvesting_api, url_prefix='/harvesting')
application.register_blueprint(clustering_api, url_prefix='/clustering')
application.register_blueprint(learning_api, url_prefix='/learning')
application.register_blueprint(extraction_api, url_prefix='/extraction')
application.register_blueprint(project_api, url_prefix='/project')

log_level = logging.INFO
logging.basicConfig(level=log_level)
logger = logging.getLogger("landmark")
# main_file = os.path.abspath(sys.modules['__main__'].__file__)
main_directory = os.path.dirname(__file__)
handler = logging.handlers.RotatingFileHandler(
              os.path.join(main_directory, 'landmark.log'), maxBytes=10*1024*1024, backupCount=5)
handler.setLevel(log_level)
formatter = logging.Formatter(u'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
application.logger.addHandler(handler)

application.config['SQLALCHEMY_POOL_SIZE'] = 100
application.config['SQLALCHEMY_POOL_RECYCLE'] = 1800
application.config['JSONIFY_PRETTYPRINT_REGULAR'] = False


def runserver():
    try:
        application.run(host='0.0.0.0', debug=True, processes=2)
    except Exception as error:
        logger.error(error)
        raise


@application.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

if __name__ == '__main__':

    if MODE == 'KAFKA_CONSUMER':
        import kafka_consumer
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO
        )
        kafka_consumer.main()
    else:
        if MODE == 'DOCKER_CONTAINER':
            reader_p = multiprocessing.Process(target=file_processor, args=())
            reader_p.daemon = True
            reader_p.start()

    runserver()
