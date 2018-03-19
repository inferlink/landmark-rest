import sys

import os
import shutil
import codecs
import json
import re

# extracts from .jl file all html that matches a url_pattern
#files are placed in file_dir (see main() for examples)
def generate_files(file_dir, jl_file, url_pattern):
    #create directory
    file_dir_str = os.path.join('.', file_dir)
    if os.path.exists(file_dir_str):
        shutil.rmtree(file_dir_str)
    os.makedirs(file_dir_str)

    r = re.compile(url_pattern)

    #read jl_file
    file_count = 1
    with codecs.open(jl_file, "r", "utf-8") as myfile:
        lines = myfile.readlines()
        for line in lines:
            json_object = json.loads(line)
            if 'url' in json_object:
                url = json_object['url']
                #apply pattern
                if r.match(url):
                    #print "matched:", url
                    page_str = json_object['raw_content']
                    page_id = format(file_count, '05')
                    file_count += 1
                    with codecs.open(os.path.join(file_dir_str, page_id + ".html"), "w", "utf-8") as myfile:
                        myfile.write(page_str)
            else:
                print 'ERROR: Document has no URL!'


def main(argv):

    #usage
    # python generate_cluster_files.py 'test_dir' '/Users/mariamuslea/Documents/landmark-ui-feb_2017_reorg/landmark_ui/angular_flask/static/project_folders/shooterswap/collect/shooterswap_maria_small.jl' 'http://shooterswap\..*'

    # r = re.compile('http://bla.blu/[^/]+$')
    # if r.match("http://bla.blu/ghg"):
    #     print 'true';
    # sys.exit()

    file_dir = argv[1]
    jl_file = argv[2]
    pattern = argv[3]

    generate_files(file_dir, jl_file, pattern)

    # python
    # generate_cluster_files.py
    # 'mx_hotstockednews_cluster5' '/home/ubuntu/memex-crawler/output/mx_hotstockednews.jl' 'http://newsletter.hotstocked.com/newsletters/view/.*'
    #
    # python
    # generate_cluster_files.py
    # 'mx_hotstockedidx_cluster4' '/home/ubuntu/memex-crawler/output/mx_hotstockedidx.jl' 'http://www.hotstocked.com/stock/[^/]+$'
    #
    # python
    # generate_cluster_files.py
    # 'mx_otcarchives_cluster4' '/home/ubuntu/memex-crawler/output/mx_otcarchives.jl' 'http://www.theotc.today/[0-9][0-9][0-9][0-9]/[0-9][0-9]/.*'
    #
    # python
    # generate_cluster_files.py
    # 'mx_microstockprofitblog_cluster6' '/home/ubuntu/memex-crawler/output/mx_microstockprofitblog.jl' 'http://www.microstockprofit.com/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][\
    # 0-9]/.*'

    # generate_files('test_dir',
    #                '/Users/mariamuslea/Documents/landmark-ui-feb_2017_reorg/landmark_ui/angular_flask/static/project_folders/shooterswap/collect/shooterswap_maria_small.jl',
    #                'http://shooterswap\..*')


if __name__ == "__main__":
    sys.exit(main(sys.argv))
