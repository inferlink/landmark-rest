import time
import urllib2
import chardet

class Util(object):

    @staticmethod
    def top_x_from_dict(values, top_x=5):
        # first we invert them, then sort them, then keep top 5
        sorted_list = sorted(zip(values.keys(), values.values()), key=lambda x: x[1], reverse=True)
        return sorted_list[:top_x]
    
    @staticmethod
    def now_millis():
        return int(round(time.time() * 1000))

    ##
    # This function converts an item like
    # {
    #   "item_1":"value_11",
    #   "item_2":"value_12",
    #   "item_3":"value_13",
    #   "item_4":["sub_value_14", "sub_value_15"],
    #   "item_5":{
    #       "sub_item_1":"sub_item_value_11",
    #       "sub_item_2":["sub_item_value_12", "sub_item_value_13"]
    #   }
    # }
    # To
    # {
    #   "node_item_1":"value_11",
    #   "node_item_2":"value_12",
    #   "node_item_3":"value_13",
    #   "node_item_4_0":"sub_value_14",
    #   "node_item_4_1":"sub_value_15",
    #   "node_item_5_sub_item_1":"sub_item_value_11",
    #   "node_item_5_sub_item_2_0":"sub_item_value_12",
    #   "node_item_5_sub_item_2_0":"sub_item_value_13"
    # }
    ##
    @staticmethod
    def reduce_item(reduced_item, key, value):
        #    global reduced_item

        # Reduction Condition 1
        if type(value) is list:
            i = 0
            for sub_item in value:
                Util.reduce_item(reduced_item, key + '_' + to_string(i), sub_item)
                i = i + 1

        # Reduction Condition 2
        elif type(value) is dict:
            sub_keys = value.keys()
            for sub_key in sub_keys:
                newkey = key + '_' + to_string(sub_key) if (key is not None) else to_string(sub_key)
                Util.reduce_item(reduced_item, newkey, value[sub_key])

        # Base Condition
        else:
            reduced_item[to_string(key)] = to_string(value)

    @staticmethod
    def send_email(user, pwd, recipient, subject, body):
        import smtplib

        gmail_user = user
        gmail_pwd = pwd
        FROM = user
        TO = recipient if type(recipient) is list else [recipient]
        SUBJECT = subject
        TEXT = body

        # Prepare actual message
        message = """From: %s\nTo: %s\nSubject: %s\n\n%s
        """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.ehlo()
            server.starttls()
            server.login(gmail_user, gmail_pwd)
            server.sendmail(FROM, TO, message)
            server.close()
            print 'successfully sent the mail'
        except:
            print "failed to send mail"

    @staticmethod
    def download_url(url):
        req = urllib2.Request(url, headers={'User-Agent': "Magic Browser"})
        con = urllib2.urlopen(req)
        page_contents = con.read()

        charset = chardet.detect(page_contents)
        page_encoding = charset['encoding']

        return page_contents.decode(page_encoding)

##
# Convert to string keeping encoding in mind...
##
def to_string(s):
    try:
        return str(s)
    except:
        # Change the encoding type if needed
        return s.encode('utf-8')
