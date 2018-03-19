from urlparse import urlparse
from tldextract import tldextract


class Tld(object):

    @staticmethod
    def extract_sld(url):
        sld = ''
        parse_result = urlparse(url)
        if parse_result.hostname:
            sld = parse_result.hostname.replace('www.', '', 1)

        return sld

    @staticmethod
    def extract_tld(url):
        return tldextract.extract(url).domain + '.' + tldextract.extract(url).suffix

    @staticmethod
    def check_url(url_to_check, tld):
        if tld in url_to_check:
            return True
        else:
            return False

if __name__ == '__main__':
    test = ['http://jair.org',
            'https://nodesecurity.io/advisories',
            'https://www.ibm.com/blogs/psirt/ ',
            'http://forums.bbc.co.uk:343/'
            ]
    for t in test:
        print Tld.extract_tld(t)
