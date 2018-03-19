import re
import string
import sys
import codecs
import time

PAGE_BEGIN = 'BEGINOFPAGE'
PAGE_END = 'ENDOFPAGE'
LONG_EXTRACTION_SEP = '##DONTCARE##'
DEBUG_HTML = '''
<!DOCTYPE html>
<html lang="en">
  <head>
    <title>PAGE_ID</title>
    <style>
      pre{ white-space:pre-wrap; word-wrap:break-word; }
      .stripe {background-color: green; display: inline;}
      .last-mile-stripe {background-color: purple; display: inline;}
    </style>
  </head>
  <body>
    <pre>
    DEBUG_HTML
    </pre>
  </body>
</html>
'''

class TYPE:
    EXACT, LEFT_LAST_MILE, RIGHT_LAST_MILE, BOTH_LAST_MILE, UNKNOWN = range(5)

class BoundingStripes(object):
    def __init__(self):
        ###SIZE OF text_ranges and page_ids MUST be equal
        #text_ranges[1] corresponds to page_ids[1]
        #text_range is a list of pairs [start,end]
        self.text_ranges = []
        #bounding_stripes is a pair (start_stripe, end_stripe) that is a valid stripe
        #for all pairs in text_range
        self.bounding_stripes = []
        #a list of page_ids that the text_ranges correspond to
        self.page_ids = []
        #extract_text_ranges[1] corresponds to page_ids[1]
        #extract_text_ranges is a list of pairs [start,end] that contain the start and end of the text to be extracted
        #This is built AFTER for now
        self.extract_text_ranges = {}
        self.type = TYPE.UNKNOWN
    
    def classify(self):
        #check all the ranges
        self.type = TYPE.EXACT
        for i in range(0, len(self.text_ranges)-1):
            text_start = self.text_ranges[i][0]
            text_end = self.text_ranges[i][1]
            page_id = self.page_ids[i]
            start_tuple_size = self.bounding_stripes[0]['tuple_size']
            start_stripe_loc = self.bounding_stripes[0]['page_locations'][page_id]
            end_stripe_loc = self.bounding_stripes[1]['page_locations'][page_id]
            #in this case I have TYPE.EXACT; do not remove this comment
            #if text_start == start_stripe_loc+start_tuple_size and text_end == end_stripe_loc-1:
            if start_stripe_loc+start_tuple_size < text_start:
                self.setType(TYPE.LEFT_LAST_MILE)
            if end_stripe_loc-1 > text_end:
                self.setType(TYPE.RIGHT_LAST_MILE)

    def setType(self, type):
        if type == TYPE.LEFT_LAST_MILE:
            if self.type == TYPE.EXACT:
                self.type = TYPE.LEFT_LAST_MILE
            elif self.type == TYPE.RIGHT_LAST_MILE:
                self.type = TYPE.BOTH_LAST_MILE
        elif type == TYPE.RIGHT_LAST_MILE:
            if self.type == TYPE.EXACT:
                self.type = TYPE.RIGHT_LAST_MILE
            elif self.type == TYPE.LEFT_LAST_MILE:
                self.type = TYPE.BOTH_LAST_MILE
        else:
            self.type = TYPE.UNKNOWN

    def __repr__(self):
        return "type: %s, extract_text_ranges: %s, text_ranges: %s, id: %s, stripes: %s" % (self.type, self.extract_text_ranges, self.text_ranges, self.page_ids, self.bounding_stripes)
    def __str__(self):
        return "type: %s, extract_text_ranges: %s, text_ranges: %s, id: %s, stripes: %s" % (self.type, self.extract_text_ranges, self.text_ranges, self.page_ids, self.bounding_stripes)

class Tuple(object):
    def __init__(self, tokens):
        self._tokens = tokens
        self._string = ''.join(tokens)

class Token(object):
    __slots__ = ['token', 'has_whitespace_before', 'visible', 'whitespace_text', 'token_location', 'char_location']

    def __init__(self, has_whitespace_before, visible):
        self.token = ''
        #True or False
        self.has_whitespace_before = has_whitespace_before
        #True or False
        self.visible = visible
        #the whitespace string just before this token
        self.whitespace_text = ''
        #same as index in array of Tokens "page.tokens_with_detail"
        self.token_location = -1
        self.char_location = -1
        
    def __str__(self, *args, **kwargs):
        return self.token
    
    def getTokenWithWhitespace(self):
        if self.has_whitespace_before:
            return " " + self.token
        else:
            return self.token

class TokenList(list):
    def getTokensAsString(self, start_index, stop_index, whitespace = False):
        tokens = []
        if whitespace:
            tokens = list(map(lambda x: x.getTokenWithWhitespace(), self[start_index:stop_index]))
        else:
            tokens = list(map(lambda x: x.token, self[start_index:stop_index]))
        #for index in range(start_index, stop_index):
        #    token_text = self[index].token
        #    if whitespace:
        #        token_text = self[index].getTokenWithWhitespace()
        #    tokens.append(token_text)
        return ''.join(tokens)
    
    def __str__(self, *args, **kwargs):
        return self.getTokensAsString(0, len(self))


class Page(object):
    __slots__ = ['_id', 'string', 'tokens', 'tuples_by_size', 'tuple_locations_by_size']

    def __init__(self, page_id, page_string, tuple_sizes=[6, 5, 4, 3, 2, 1], add_special_tokens = True):
        self._id = page_id
        if isinstance(page_string, str):
            page_unicode = unicode(page_string, 'utf-8', 'ignore')
        else:
            page_unicode = page_string
#         page_normal = unicodedata.normalize('NFKD', page_unicode).encode('ascii', 'ignore')
        page_normal = page_unicode
        if add_special_tokens:
            self.string = PAGE_BEGIN+' '+page_normal+' '+PAGE_END
        else:
            self.string = page_normal

        #contains Token objects; index of array represents the token location
        #tokens_with_detaul[50] = the 50th token as returned by tokenize()
        self.tokens = TokenList(tokenize(self.string))

        tokens_strings = tokenize_string(self.string)
        tuples_by_size = {}

        # largest_tuple_size = max(tuple_sizes)
        # top_range = largest_tuple_size+1
        # if len(self.tokens) < top_range:
        #     top_range = len(self.tokens)
        for size in tuple_sizes:
            # tuples = []
            # tokens = []
            # if size > 1:
            #     for j in range(0, size-1):
            #         tokens.append(self.tokens[j])
            # else:
            #     j = -1
            #     #j = 0
            # for token in self.tokens[j+1:]:
            #     tokens.append(token)
            #     tuple_string = u''
            #     for token in tokens:
            #         tuple_string = tuple_string + unicode(token.token)
            #     tuples.append(tuple_string)
            #     tokens.pop(0)

            tuples = []
            grams = zip(*[tokens_strings[i:] for i in range(size)])
            for gram in grams:
                tuple_string = ''.join(gram)
                tuples.append(tuple_string)

            tuples_by_size[size] = tuples
        self.tuples_by_size = tuples_by_size
        
        # for size in range(top_range, largest_tuple_size+1):
        #     tuples_by_size[size] = []

        self.tuple_locations_by_size = {}
        # for size in range(1, largest_tuple_size+1):
        for size in tuple_sizes:
            count = 0
            tuple_locations = {}
            for tuple_iter in self.tuples_by_size[size]:
                if tuple_iter not in tuple_locations:
                    tuple_locations[tuple_iter] = []
                tuple_locations[tuple_iter].append(count)
                count = count + 1
            self.tuple_locations_by_size[size] = tuple_locations

    def getId(self):
        return self._id
    
    def getString(self):
        return self.string
    
    def uniqueOnPage(self, sub_string):
        string_unicode = unicode(sub_string, 'utf-8', 'ignore')
#         string_normal = unicodedata.normalize('NFKD', string_unicode).encode('ascii', 'ignore')
        string_normal = string_unicode
        indexes = [[m.start(), m.start()+len(string_normal)] for m in re.finditer(re.escape(string_normal), self.string)]
        if len(indexes) == 1:
            return indexes[0]
        elif len(indexes) < 1:
            return None
        return [-1,-1]
    
    def number_of_times_on_page(self, stripe, interval):
        page_sub_string = self.tokens.getTokensAsString(interval[0], interval[1])
        matches = re.findall(re.escape(stripe['stripe']), page_sub_string)
        return len(matches)
    
    def get_location(self, tuple_size, token):
        try:
            location = self.tuple_locations_by_size[tuple_size][unicode(token)]
        except KeyError, e:
                location = []
        return location
    
    def get_token(self, index):
        return self.tokens[index]

def tokenize(text):
    all_tokens = list(filter(lambda x: x, re.split('((?:\\s+)|(?:<[^\\s\W_]+)|(?:</[^\\s\W_]+>)|\W|_)', text)))
    final_tokens = []
    whitespace_token = ''
    # BEGINOFPAGE is token 0
    token_index = 0
    char_location = 0

    in_tag = False
    in_script = False
    in_style = False
    last_was_gt = False
    last_was_lt = False
    last_was_lt_then_slash = False

    for token in all_tokens:
        visible = True

        if last_was_gt:
            in_tag = False
            last_was_gt = False
        elif last_was_lt:
            if token.isspace():
                in_tag = False
            last_was_lt = False
        if token.lower() == '</style>':
            in_style = False
        elif token.lower() == '</script>':
            in_script = False

        if token == '<':
            in_tag = True
            last_was_lt = True
        elif token == '<style':
            in_tag = True
            in_style = True
        elif token == '<script':
            in_tag = True
            in_script = True
        else:
            if token[0] == '<':
                in_tag = True
            if token[-1] == '>':
                last_was_gt = True

        if token.isspace():
            whitespace_token = token
            char_location = char_location + len(token)
            continue
        if whitespace_token:
            if in_tag or in_script or in_style:
                new_token_obj = Token(True, False)
            else:
                new_token_obj = Token(True, visible)
            new_token_obj.whitespace_text = whitespace_token
        else:
            if in_tag or in_script or in_style:
                new_token_obj = Token(False, False)
            else:
                new_token_obj = Token(False, visible)

        new_token_obj.token_location = token_index
        new_token_obj.token = token
        new_token_obj.char_location = char_location
        char_location = char_location + len(token)
        whitespace_token = ''

        final_tokens.append(new_token_obj)

        token_index = token_index + 1

    return final_tokens

def tokenize_old(text):
    tokens = re.split('(\\s+)', text)
    all_tokens = []
    for token in tokens:
        if token.isspace():
            all_tokens.append(token)
            continue
        index = 0
        new_token = ''
        for ch in token:
            if ch in string.punctuation:
                if new_token:
                    all_tokens.append(new_token)
                new_token = ch
                all_tokens.append(new_token)
                new_token = ''
            else:
                new_token = new_token + ch
            index = index + 1
        if new_token:
            all_tokens.append(new_token)
    
    final_tokens = []
    whitespace_token = ''
    #BEGINOFPAGE is token 0
    token_index = 0
    char_location = 0
    
    in_tag = False
    in_script = False
    in_style = False
    last_was_gt = False
    last_was_lt = False
    last_was_lt_then_slash = False
    
    for token in all_tokens:
        visible = True
        
        if last_was_gt:
            in_tag = False
            last_was_gt = False
        elif last_was_lt:
            if token == '/':
                last_was_lt_then_slash = True
            elif token.lower() == 'style':
                in_style = True
            elif token.lower() == 'script':
                in_script = True
            elif token.isspace():
                in_tag = False
            last_was_lt = False
        elif last_was_lt_then_slash:
            if token.lower() == 'style':
                in_style = False
            elif token.lower() == 'script':
                in_script = False
            last_was_lt_then_slash = False
 
        if token == '<':
            in_tag = True
            last_was_lt = True
        elif token == '>':
            last_was_gt = True
        
        if token.isspace():
            whitespace_token = token
            char_location = char_location + len(token)
            continue
        if whitespace_token:
            if in_tag or in_script or in_style:
                new_token_obj = Token(True, False)
            else:
                new_token_obj = Token(True, visible)
            new_token_obj.whitespace_text = whitespace_token
        else:
            if in_tag or in_script or in_style:
                new_token_obj = Token(False, False)
            else:
                new_token_obj = Token(False, visible)

        new_token_obj.token_location = token_index
        new_token_obj.token = token
        new_token_obj.char_location = char_location
        char_location = char_location + len(token)
        whitespace_token = ''

        final_tokens.append(new_token_obj)

        token_index = token_index +1
        
    return final_tokens

def tokenize_tuple(text):
    all_tokens = list(filter(lambda x: x, re.split('((?:\\s+)|(?:<[^\\s\W_]+)|(?:</[^\\s\W_]+>)|\W|_)', text)))
    final_tokens = []
    whitespace_token = ''
    # BEGINOFPAGE is token 0
    token_index = 0
    char_location = 0

    in_tag = False
    in_script = False
    in_style = False
    last_was_gt = False
    last_was_lt = False

    for token in all_tokens:
        visible = True

        if last_was_gt:
            in_tag = False
            last_was_gt = False
        elif last_was_lt:
            if token.isspace():
                in_tag = False
            last_was_lt = False
        if token.lower() == '</style>':
            in_style = False
        elif token.lower() == '</script>':
            in_script = False

        if token == '<':
            in_tag = True
            last_was_lt = True
        elif token == '<style':
            in_tag = True
            in_style = True
        elif token == '<script':
            in_tag = True
            in_script = True
        else:
            if token[0] == '<':
                in_tag = True
            if token[-1] == '>':
                last_was_gt = True

        if token.isspace():
            whitespace_token = token
            char_location = char_location + len(token)
            continue
        if whitespace_token:
            if in_tag or in_script or in_style:
                new_token_obj = (False, token, whitespace_token)
            else:
                new_token_obj = (visible, token, whitespace_token)
        else:
            if in_tag or in_script or in_style:
                new_token_obj = (False, token, None)
            else:
                new_token_obj = (visible, token, None)

        whitespace_token = ''
        final_tokens.append(new_token_obj)
        token_index = token_index + 1

    return final_tokens

def tokenize_tuple_old(text):
    tokens = re.split('(\\s+)', text)
    all_tokens = []
    for token in tokens:
        if token.isspace():
            all_tokens.append(token)
            continue
        index = 0
        new_token = ''
        for ch in token:
            if ch in string.punctuation:
                if new_token:
                    all_tokens.append(new_token)
                new_token = ch
                all_tokens.append(new_token)
                new_token = ''
            else:
                new_token = new_token + ch
            index = index + 1
        if new_token:
            all_tokens.append(new_token)

    final_tokens = []
    whitespace_token = ''
    # BEGINOFPAGE is token 0
    token_index = 0
    char_location = 0

    in_tag = False
    in_script = False
    in_style = False
    last_was_gt = False
    last_was_lt = False
    last_was_lt_then_slash = False

    for token in all_tokens:
        visible = True

        if last_was_gt:
            in_tag = False
            last_was_gt = False
        elif last_was_lt:
            if token == '/':
                last_was_lt_then_slash = True
            elif token.lower() == 'style':
                in_style = True
            elif token.lower() == 'script':
                in_script = True
            elif token.isspace():
                in_tag = False
            last_was_lt = False
        elif last_was_lt_then_slash:
            if token.lower() == 'style':
                in_style = False
            elif token.lower() == 'script':
                in_script = False
            last_was_lt_then_slash = False

        if token == '<':
            in_tag = True
            last_was_lt = True
        elif token == '>':
            last_was_gt = True

        if token.isspace():
            whitespace_token = token
            char_location = char_location + len(token)
            continue
        if whitespace_token:
            if in_tag or in_script or in_style:
                new_token_obj = (False, token, whitespace_token)
            else:
                new_token_obj = (visible, token, whitespace_token)
        else:
            if in_tag or in_script or in_style:
                new_token_obj = (False, token, None)
            else:
                new_token_obj = (visible, token, None)

        whitespace_token = ''
        final_tokens.append(new_token_obj)
        token_index = token_index + 1

    return final_tokens

def tokenize_string(text):
    all_tokens = list(filter(lambda x: x, re.split('((?:\\s+)|(?:<[^\\s\W_]+)|(?:</[^\\s\W_]+>)|\W|_)', text)))

    final_tokens = []

    for token in all_tokens:
        if token.isspace():
            continue
        final_tokens.append(token)

    return final_tokens

def tokenize_string_old(text):
    tokens = re.split('(\\s+)', text)
    all_tokens = []
    for token in tokens:
        if token.isspace():
            all_tokens.append(token)
            continue
        index = 0
        new_token = ''
        for ch in token:
            if ch in string.punctuation:
                if new_token:
                    all_tokens.append(new_token)
                new_token = ch
                all_tokens.append(new_token)
                new_token = ''
            else:
                new_token = new_token + ch
            index = index + 1
        if new_token:
            all_tokens.append(new_token)

    final_tokens = []

    for token in all_tokens:
        if token.isspace():
            continue
        final_tokens.append(token)

    return final_tokens


def removeHtml(tokens):
    new_tokens = []
    add_token = True
    for token in tokens:
        if token != '<' and add_token == True:
            new_tokens.append(token)
        elif token == '<':
            add_token = False
        elif token == '>':
            add_token = True;
    return new_tokens

class HarvestInfo(object):

    def __init__(self, status, pages_fetched, pages_failed):
        self.status = status
        self.pages_fetched = pages_fetched
        self.pages_failed = pages_failed

if __name__ == '__main__':
    with codecs.open(sys.argv[1], "rU", "utf-8") as myfile:
        for token in tokenize_string(myfile.read().encode('utf-8')):
            print token