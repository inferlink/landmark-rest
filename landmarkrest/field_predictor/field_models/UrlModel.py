from BaseModel import BaseModel
import re


class UrlModel(BaseModel):

    def __init__(self):
        super(UrlModel, self).__init__()

        # this is Django's url regex
        self.__url_regex = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):

        matches = [z for z in slot_values if re.search(self.__url_regex, z) is not None]

        confidence = float(len([z for z in matches if z[1]])) / float(len(slot_values))

        return confidence
