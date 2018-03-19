import logging, time, traceback, json, codecs
import multiprocessing
from kafka import KafkaConsumer
from landmarkrest.util.tld import Tld
from landmarkrest.project import run_isi_workflow
from landmarkrest.data.models import Project, Harvest
from landmarkrest.util.util import Util
from database import db_session
from collections import OrderedDict
from settings import *

queue = multiprocessing.Queue()


def file_len(file_name):
    if os.path.exists(file_name):
        with open(file_name) as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    return 0


def file_processor():
    while True:
        file_location = queue.get()         # Read from the queue and do nothing
        variables = file_location.split('--==--')
        process_file(variables[0], variables[1], variables[2])


def process_file(project_name, tld, jl_file):
    landmark_project_name = project_name + '-_-' + tld
    logging.info('Processing file for ' + str(jl_file))
    now = time.gmtime()
    project = Project(landmark_project_name + "-" + time.strftime("%Y%m%d_%H%M%S", now))
    db_session.add(project)
    db_session.flush()

    # now we have a project, let's create a harvest for this thing and then start the work.
    depth = -1
    prefer_pagination = False
    multi_urls = ""
    concurrent_requests = -1
    concurrent_requests_per_domain = -1
    duration = -1
    error_page_percentage = -1
    error_page_percentage_period = -1

    # Create a Harvest
    db_harvest = Harvest(project.id, 'http://' + tld, '', depth,
                         prefer_pagination, multi_urls,
                         concurrent_requests, concurrent_requests_per_domain, duration,
                         error_page_percentage,
                         error_page_percentage_period, Util.now_millis())
    db_session.add(db_harvest)
    db_harvest.crawl_id = ISI_HARVEST_PREFIX + landmark_project_name
    db_harvest.pages_fetched = file_len(jl_file)
    db_harvest.jl_file_location = jl_file
    db_session.flush()
    db_session.commit()

    run_isi_workflow(project.id)

    # process = multiprocessing.Process(target=run_isi_workflow,
    #                                   args=[project.id])
    # process.start()


def process_message(message):
    try:
        message_payload = json.loads(message.value, object_pairs_hook=OrderedDict)
        url = message_payload['url']
        logging.info('Processing message for ' + str(url))
        tld = Tld.extract_tld(url)

        project_name = message.topic
        jl_file = project_name + '-_-' + tld + '.jl'
        jl_file_location = os.path.join(LOCAL_S3_DIR, jl_file)
        file_line_count = file_len(jl_file_location)

        if file_line_count < PAGES_PER_SITE:
            # add this to the end with our own "fake_id"
            message_payload['_id'] = uuid.uuid4().hex + '_' + str(file_line_count+1)
            message_payload.pop('objects', None)

            with codecs.open(jl_file_location, 'a+', 'utf-8') as myfile:
                myfile.write(json.dumps(OrderedDict(message_payload)) + '\n')

            # if the length is now 100:
            if file_line_count == PAGES_PER_SITE-1:
                # then process it!
                queue.put(project_name + '--==--' + tld + '--==--' + jl_file_location)
                # process_file(project_name, tld, jl_file_location)
    except:
        logging.error('Failed to process message')
        logging.error(traceback.format_exc())


class Consumer(multiprocessing.Process):
    daemon = True

    def run(self):

        brokers = KAFKA_CONNECTIONS.split()
        topics = KAFKA_CONSUME_TOPICS.split()

        if KAFKA_SECURITY_PROTOCOL == 'SSL':
            consumer = KafkaConsumer(
                bootstrap_servers=brokers,
                security_protocol='SSL',
                ssl_cafile=KAFKA_SSL_CAFILE,
                ssl_certfile=KAFKA_SSL_CERTFILE,
                ssl_keyfile=KAFKA_SSL_KEYFILE,
                ssl_check_hostname=False,
                auto_offset_reset='earliest',
                group_id=KAFKA_GROUP_ID,
            )
        else:
            consumer = KafkaConsumer(bootstrap_servers=brokers,
                                     auto_offset_reset='earliest',
                                     group_id=KAFKA_GROUP_ID,
                                     )

        consumer.subscribe(topics)

        for message in consumer:
            process_message(message)


def main():
    tasks = [
        Consumer()
    ]

    for t in tasks:
        t.start()

    reader_p = multiprocessing.Process(target=file_processor, args=())
    reader_p.daemon = True
    reader_p.start()
    reader_p.join()

    time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
