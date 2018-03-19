import string
import time
import json
import re
import os
import codecs
from itertools import groupby
from operator import itemgetter
from collections import OrderedDict
from common import Page, tokenize, TokenList, LONG_EXTRACTION_SEP, PAGE_BEGIN, PAGE_END,\
    removeHtml, BoundingStripes, TYPE, DEBUG_HTML
from landmarkrest.field_predictor.FieldPredictor import FieldPredictor
from landmark_extractor.extraction.Landmark import RuleSet, ItemRule, IterationRule, flattenResult,\
    escape_regex_string
from landmark_extractor.postprocessing.PostProcessor import RemoveHtml
import random
import copy

import logging
logger = logging.getLogger("landmark")

cachedStopWords = ['.', ',', ':', '@', '.com', '.net', '@gmail.com']

list_tags = {
           '<dl': ['</dl>', '<dt', '</dd>']
         , '<ul': ['</ul>', '<li', '</li>']
         , '<ol': ['</ol>', '<li', '</li>']
         , '<table': ['</table>', '<tr', '</tr>']
    }

ignore_items = ['begin_index', 'end_index', 'starting_token_location', 'ending_token_location', 'extract',
                'sequence', 'sequence_number', 'rule_id']

LAST_MILE_LEVEL = 9999

class PageManager(object):
    __unique_value = 999999

    def __init__(self, write_debug_files=False, tuple_sizes=[20, 10, 5, 3, 2, 1], page_dir=None, ignore_files=[],
                 auto_learn_sub_rules=False, do_field_prediction=True, remove_bad_rules=False, find_last_miles=False):
        self._pages = {}
        self.seed_page_id = None
        self.seed_interval = None
        self.max_level = 1
        self._WRITE_DEBUG_FILES = write_debug_files
        self._debug_template_html = {}
        if not tuple_sizes:
            tuple_sizes = [6, 5, 4, 3, 2, 1]
        self.tuple_sizes = sorted(tuple_sizes, reverse=True)
        self.largest_tuple_size = tuple_sizes[0]
        self._AUTO_LEARN_SUB_RULES = auto_learn_sub_rules
        self._DO_FIELD_PREDICTION = do_field_prediction
        self._REMOVE_BAD_RULES = remove_bad_rules
        self._FIND_LAST_MILES = find_last_miles
        
        if page_dir:
            files = [f for f in os.listdir(page_dir) if os.path.isfile(os.path.join(page_dir, f))]
            for the_file in files:
                if the_file.startswith('.') or the_file in ignore_files:
                    continue
    
                with codecs.open(os.path.join(page_dir, the_file), "rU", "utf-8") as myfile:
                    page_str = myfile.read().encode('utf-8')
                self.addPage(the_file, page_str)

    def get_pages(self):
        return self._pages
    
    @staticmethod
    def getTemplateFromTemplates(stripes, templates=[], pages=[], cluster_name=None):
        templateFromTemplates = PageManager()

        for page in pages:
            templateFromTemplates.addPage(page.getId(), page=page.getString(), add_special_tokens=False)

        count = 1
        for template in templates:
            templateFromTemplates.addPage('template'+str(count), page=template, add_special_tokens=False)
            count += 1
            
        templateFromTemplates.learnStripes()
        
        if cluster_name:
            with codecs.open('tfromt_debug'+cluster_name+'.html', "w", "utf-8") as myfile:
                output_html = DEBUG_HTML\
                    .replace('PAGE_ID', cluster_name)\
                    .replace('DEBUG_HTML',
                             templateFromTemplates.getDebugOutputBuffer(stripes, templateFromTemplates.seed_page_id))
                myfile.write(output_html)
                myfile.close()
        return templateFromTemplates

    # TODO: BA FIX FOR NEW STRIPES
    def getFilledInTemplate(self, stripes):
        template_buffer = ''
        seed_page = self.getPage(self.seed_page_id)
        previous_stripe = None
        if stripes:
            for stripe in stripes:
                if previous_stripe:
                    if(previous_stripe['page_locations'][self.seed_page_id] + previous_stripe['tuple_size'] != stripe['page_locations'][self.seed_page_id]):
                        template_buffer += ' ' + str(PageManager.__unique_value) + ' '
                        PageManager.__unique_value += 1
#                         template_buffer += ' VALUE' + ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(16)) + ' '
                stripe_string = seed_page.tokens.getTokensAsString(\
                            stripe['page_locations'][self.seed_page_id], stripe['page_locations'][self.seed_page_id]+stripe['tuple_size'], whitespace = True)
                template_buffer += stripe_string
                previous_stripe = stripe
            
        return template_buffer
    
    def getSubPageManager(self, page_ids=[], template_string='', cluster_name=None):
        sub_page_manager = PageManager()
        
        for page_id in page_ids:
            curr_page = self.getPage(page_id)
            sub_page_manager.addPage(page_id, curr_page.string, add_special_tokens=False)
        stripes = sub_page_manager.learnStripes()
        
        if template_string:
            sub_page_manager.addPage('template_string', template_string, add_special_tokens=False)
        
        if cluster_name:
            with codecs.open('debug'+cluster_name+'.html', "w", "utf-8") as myfile:
                output_html = DEBUG_HTML\
                    .replace('PAGE_ID', cluster_name)\
                    .replace('DEBUG_HTML',
                             sub_page_manager.getDebugOutputBuffer(stripes, sub_page_manager.seed_page_id))
                myfile.write(output_html)
                myfile.close()
        
        return sub_page_manager, stripes
            
    def getPageChunks(self, page_id):
        chunks = []
        page = self.getPage(page_id)
        previous_visible = False
        invisible_token_buffer_before = [] #""
        visible_token_buffer = [] #""
        for token in page.tokens:
            if token.token == PAGE_BEGIN or token.token == PAGE_END:
                continue
            if token.visible:
#                 if token.whitespace_text and previous_visible:
                visible_token_buffer.append(token.token)
                previous_visible = True
            elif previous_visible:
                previous_visible = False
                chunks.append(' '.join(visible_token_buffer))
                invisible_token_buffer_before = []
                visible_token_buffer = []
                
                if token.whitespace_text and not previous_visible:
                    invisible_token_buffer_before.append(token.token)
            else:
                if token.whitespace_text and not previous_visible:
                    invisible_token_buffer_before.append(token.token)
        return set(chunks)

    def getVisibleTokenStructure(self, data_as_strings=True, data_as_tokens=False):
        datas = []
        for page_id in self._pages:
            page = self.getPage(page_id)
            previous_visible = False
            invisible_token_buffer_before = []
            visible_token_buffer = []
            first_invis_token = None
            first_vis_token = None
            for token in page.tokens:
                if token.token == PAGE_BEGIN or token.token == PAGE_END:
                    continue
                if token.visible:
                    if token.whitespace_text and previous_visible:
                        if not data_as_tokens:
                            visible_token_buffer.append(' ')
                    if data_as_tokens:
                        visible_token_buffer.append(token)
                    else:
                        visible_token_buffer.append(token.token)
                    if first_vis_token is None:
                        first_vis_token = token
                    previous_visible = True
                elif previous_visible:
                    previous_visible = False
                    if data_as_strings:
                        datas.append({"page_id": page_id, "visible_token_buffer": ''.join(visible_token_buffer),
                                    "invisible_token_buffer_before": ''.join(invisible_token_buffer_before),
                                      "first_vis_token": first_vis_token, "first_invis_token": first_invis_token})
                    else:
                        datas.append({"page_id": page_id, "visible_token_buffer": visible_token_buffer,
                                        "invisible_token_buffer_before": invisible_token_buffer_before,
                                      "first_vis_token": first_vis_token, "first_invis_token": first_invis_token})

                    invisible_token_buffer_before = []
                    visible_token_buffer = []
                    first_invis_token = None
                    first_vis_token = None
                    
                    if token.whitespace_text and not previous_visible:
                        invisible_token_buffer_before.append(' ')
                    invisible_token_buffer_before.append(token.token)
                    if first_invis_token is None:
                        first_invis_token = token
                else:
                    if token.whitespace_text and not previous_visible:
                        invisible_token_buffer_before.append(' ')
                    invisible_token_buffer_before.append(token.token)
                    if first_invis_token is None:
                        first_invis_token = token
        return datas
    
    def getVisibleTokenBuffer(self, page_id):
        page = self.getPage(page_id)
        previous_visible = False
        token_buffer = ""
        for token in page.tokens:
            if token.token == PAGE_BEGIN or token.token == PAGE_END:
                continue
#             token_buffer += token.token + " - " + str(token.visible) + "\n"
            if token.visible:
                if token.whitespace_text and previous_visible:
                    token_buffer += ' '
                token_buffer += token.token
                previous_visible = True
            elif previous_visible:
                previous_visible = False
                token_buffer += "\n-------------\n"
        return token_buffer
    
    def getStripeFragmentsForSlot(self, stripes, start_stripe, direction='begin'):
        stripe_fragments = []
        stripes_to_check = []
        if direction == 'begin':
            stripes_to_check = reversed(stripes[:stripes.index(start_stripe)+1])
        elif direction == 'end':
            stripes_to_check = stripes[stripes.index(start_stripe):]

        previous_loc = start_stripe['page_locations'].values()[0] + start_stripe['tuple_size']
        for stripe in stripes_to_check:
            if stripe['page_locations'].values()[0] + stripe['tuple_size'] == previous_loc:
                stripe_fragments.insert(0, stripe)
                previous_loc = stripe['page_locations'].values()[0]
            else:
                break
            
        return stripe_fragments
    
    # def countTokenInfoInStripesAndCleanSlots(self, stripes, page_id):
    #     if not page_id:
    #         page_id = self.seed_page_id
    #
    #     visible_count = 0
    #     invisible_count = 0
    #     clean_slots_info = []
    #     page = self.getPage(page_id)
    #
    #     previous_stripe = None
    #
    #     for stripe in self._stripes:
    #         for token_index in range(stripe['page_locations'][page_id], stripe['page_locations'][page_id]+stripe['tuple_size']):
    #             if page.get_token(token_index).visible:
    #                 visible_count += 1
    #             else:
    #                 invisible_count += 1
    #         if previous_stripe:
    #             good_slot = False
    #             slot_start = previous_stripe['page_locations'][page_id]+previous_stripe['tuple_size']
    #             slot_end = stripe['page_locations'][page_id]
    #             if(previous_stripe['page_locations'][page_id] + previous_stripe['tuple_size'] != stripe['page_locations'][page_id]):
    #                 good_slot = True
    #                 for token_index in range(slot_start, slot_end):
    #                     current_token = page.get_token(token_index)
    #                     if not current_token.visible:
    #                         good_slot = False
    #                         break
    #             if good_slot:
    #                 clean_slots_info.append( (slot_start, slot_end-1) )
    #         previous_stripe = stripe
    #
    #     return (visible_count, invisible_count, clean_slots_info)

    def getDebugOutputBuffer(self, stripes, page_id):
        counter = 0
        start_index = 0
        page = self.getPage(page_id)
        page_string = page.getString().replace(PAGE_BEGIN, '').replace(PAGE_END, '')
        output_buffer = ''
        for stripe in stripes:
            test_page = page_string[start_index:]
            test_stripes = list()
            test_stripes.append(stripe)
            test_rule = self.buildRule(test_stripes)
            finder = re.compile(test_rule, re.S)
            match = finder.search(test_page)
            if match and match.start() >= 0:
                output_buffer = output_buffer + test_page[0:match.start()].replace('<', '&lt;')
                if stripe['level'] == LAST_MILE_LEVEL:
                    output_buffer = output_buffer + "<pre class='last-mile-stripe' title='Stripe "+str(counter)+" / Level "+str(stripe['level'])+"'>"
                else:
                    opacity = self.max_level/(1.0 * stripe['level'] * self.max_level)
                    output_buffer = output_buffer + "<pre class='stripe' style='opacity:"+ str(opacity)+ ";' title='Stripe "+str(counter)+" / Level "+str(stripe['level'])+"'>"
                output_buffer = output_buffer + test_page[match.start():match.end()].replace('<', '&lt;')
                output_buffer = output_buffer + "</pre>"
                start_index = start_index + match.end()
            counter = counter + 1
        output_buffer = output_buffer + page_string[start_index:].replace('<', '&lt;')
        return output_buffer

    def learnListRulesFromLocations(self, page_markups, page_stripes):
        random.seed(5)
        page_ids = list()
        keys = list()
        for page_markup in page_markups:
            page_ids.append(page_markup)
            keys.extend(page_markups[page_markup])
        keys = list(set(keys))

        key_based_markup = {}
        for key in keys:
            if key not in key_based_markup:
                key_based_markup[key] = {}
            for page_id in page_ids:
                if key not in page_markups[page_id]:
                    del key_based_markup[key]
                    break
                elif 'sequence' in page_markups[page_id][key]:
                    key_based_markup[key][page_id] = page_markups[page_id][key]

        list_stripes = {}
        for key in key_based_markup:

            #find bounding stripes for the list
            start_stripe = None
            prev_stripe = None
            for stripe in page_stripes:
                if prev_stripe and not start_stripe:
                    for page_id1 in key_based_markup[key]:
                        interval1 = (0, len(self.getPage(page_id1).tokens)-1, page_id1)
                        if key_based_markup[key][page_id1]['starting_token_location'] < \
                                                stripe['page_locations'][interval1] + stripe['tuple_size']:
                            start_stripe = prev_stripe
                            #print start_stripe
                            #print start_stripe['stripe']
                            for page_id2 in key_based_markup[key]:
                                interval2 = (0, len(self.getPage(page_id2).tokens)-1, page_id2)
                                key_based_markup[key][page_id2]['starting_token_location'] = \
                                    start_stripe['page_locations'][interval2] + start_stripe['tuple_size']
                            break
                elif start_stripe:
                    at_end = True
                    for page_id in key_based_markup[key]:
                        interval = (0, len(self.getPage(page_id).tokens) - 1, page_id)
                        if key_based_markup[key][page_id]['ending_token_location'] >= \
                                                stripe['page_locations'][interval] + stripe['tuple_size']:
                            at_end = False
                            break
                    if at_end:
                        for page_id in key_based_markup[key]:
                            interval = (0, len(self.getPage(page_id).tokens) - 1, page_id)
                            key_based_markup[key][page_id]['ending_token_location'] = stripe['page_locations'][
                                                                                          interval] + stripe[
                                                                                          'tuple_size']
                        list_stripes[key] = (start_stripe, stripe)
                        break
                prev_stripe = stripe

        rules = RuleSet()
        for number, key in enumerate(key_based_markup):
            if key not in list_stripes:
                continue

            first_intervals = {}
            iter_intervals = {}
            row_intervals = {}
            list_bounding_stripes = BoundingStripes()
            list_bounding_stripes.bounding_stripes = list_stripes[key]
            for page in key_based_markup[key]:
                #select a random sample of rows to learn iteration rules and subrules on
                if len(key_based_markup[key][page]['sequence']) > 2:
                    between_sample = random.sample(xrange(1, len(key_based_markup[key][page]['sequence'])-1),
                                                   min(5, len(key_based_markup[key][page]['sequence'])-2))
                    for index in between_sample:
                        interval = (key_based_markup[key][page]['sequence'][index]['starting_token_location'],
                                    key_based_markup[key][page]['sequence'][index+1]['starting_token_location'], page)
                        iter_intervals[interval] = interval
                        row_intervals[interval] = interval
                interval = (key_based_markup[key][page]['starting_token_location'],
                            key_based_markup[key][page]['sequence'][0]['starting_token_location'], page)
                if interval[0] < interval[1]:
                    first_intervals[interval] = interval

                interval = (key_based_markup[key][page]['sequence'][-1]['starting_token_location'],
                            key_based_markup[key][page]['ending_token_location'], page)
                row_intervals[interval] = interval
                #print self.getPage(interval[2]).tokens.getTokensAsString(interval[0],interval[1])

                list_bounding_stripes.page_ids.append(page)
                list_bounding_stripes.text_ranges.append((key_based_markup[key][page]['starting_token_location'],
                                                          key_based_markup[key][page]['ending_token_location']))

            if len(iter_intervals) < 2:
                continue

            iter_bound_stripes = BoundingStripes()
            iter_intervals_stripes = self.learnStripes(iter_intervals)
            #for interval in begin_intervals:
            #    print '\n' + key
            #    print interval[2]
            #    print self.getPage(interval[2]).tokens.getTokensAsString(interval[0], interval[1], True)
            if not iter_intervals_stripes:
                continue
            iter_bound_stripes.bounding_stripes = (iter_intervals_stripes[0], iter_intervals_stripes[-1])
            for page_id in iter_intervals:
                iter_bound_stripes.page_ids.append(page_id)
                iter_bound_stripes.text_ranges.append((iter_intervals_stripes[0]['page_locations'][page_id] + iter_intervals_stripes[0]['tuple_size'],
                                                        iter_intervals_stripes[-1]['page_locations'][page_id]))

            first_bound_stripes = BoundingStripes()
            first_stripes = None
            if len(first_intervals) == len(key_based_markup[key]):
                first_stripes = self.learnStripes(first_intervals)
                if first_stripes:
                    first_bound_stripes.bounding_stripes = (first_stripes[0], first_stripes[-1])
                    for interval in first_intervals:
                        first_bound_stripes.page_ids.append(interval)
                        first_bound_stripes.text_ranges.append((first_stripes[0]['page_locations'][interval] + first_stripes[0]['tuple_size'],
                                                          first_stripes[-1]['page_locations'][interval]))

            row_stripes = self.learnStripes(row_intervals)
            if not row_stripes:
                continue

            iter_regex = self.buildRule(self.getNextLevelStripes(iter_intervals_stripes, iter_bound_stripes, 'end', True))
            begin_stripes = self.getNextLevelStripes(page_stripes, list_bounding_stripes, 'begin', True)
            end_stripes = self.getNextLevelStripes(page_stripes, list_bounding_stripes, 'end', True)
            begin_regex = self.buildRule(begin_stripes)
            end_regex = self.buildRule(end_stripes)

            if begin_regex and end_regex and iter_regex:
                if first_stripes:
                    first_row_regex = self.buildRule(self.getNextLevelStripes(first_stripes, first_bound_stripes, 'end', True))
                    if first_row_regex:
                        begin_regex = begin_regex + '.*?' + first_row_regex
                rule = IterationRule('list_' + str(number), begin_regex, end_regex, iter_regex,
                                     include_end_regex=True, no_first_begin_iter_rule=True, no_last_end_iter_rule=True)
                #for interval in row_intervals:
                #    print self.getPage(interval[2]).tokens.getTokensAsString(interval[0], interval[1], True)
                subrules = self.learnAllRules(row_stripes, in_list=True, outer_slots=True)
                rule.set_sub_rules(subrules)
                rules.add_rule(rule)
        return rules

    def learnListRules(self, page_stripes):
        random.seed(4)
        list_locations = []
        seed_page = self.getPage(self.seed_page_id)
        seed_interval = (0, len(self.getPage(self.seed_page_id).tokens)-1, self.seed_page_id)
        depth = 0
        cur_tag = None
        start_loc = None
        stripe_iter = iter(page_stripes)
        stripe = stripe_iter.next()
        start_stripe = None
        #assumption: each relevant list has at least one nonempty slot on the seed page
        for token in seed_page.tokens:
            if depth > 0:
                if token.token == cur_tag:
                    depth += 1
                elif token.token == list_tags[cur_tag][0]:
                    depth -= 1
                if depth == 0:
                    continuous = True
                    try:
                        while token.token_location >= stripe['page_locations'][seed_interval] + stripe['tuple_size']:
                            prev_stripe = stripe
                            stripe = stripe_iter.next()
                            if prev_stripe['page_locations'][seed_interval] + prev_stripe['tuple_size'] < stripe['page_locations'][seed_interval]:
                                continuous = False
                    except StopIteration:
                        break
                    if token.token_location >= stripe['page_locations'][seed_interval] and not continuous:
                        bounding_stripes = BoundingStripes()
                        bounding_stripes.bounding_stripes = (start_stripe, stripe)
                        start_pos_in_stripe = start_loc - start_stripe['page_locations'][seed_interval]
                        end_pos_in_stripe = token.token_location - stripe['page_locations'][seed_interval]
                        for page_id in self._pages:
                            interval_id = (0, len(self.getPage(page_id).tokens)-1, page_id)
                            bounding_stripes.page_ids.append(page_id)
                            bounding_stripes.text_ranges.append((start_stripe['page_locations'][interval_id] + \
                                    start_pos_in_stripe, stripe['page_locations'][interval_id] + end_pos_in_stripe))
                        list_locations.append({'stripes': bounding_stripes, 'tag': cur_tag})
                        #list_locations.append((start_loc, token.token_location, start_stripe, stripe))
            elif token.token in list_tags:
                try:
                    while token.token_location >= stripe['page_locations'][seed_interval]+stripe['tuple_size']:
                        stripe = stripe_iter.next()
                except StopIteration:
                    break
                if token.token_location >= stripe['page_locations'][seed_interval]:
                    cur_tag = token.token
                    depth = 1
                    start_loc = token.token_location
                    start_stripe = stripe

        for list in list_locations:
            bounding_stripes = list['stripes']
            start_tag = list['tag']
            row_locations = {}
            for page_id, text_range in zip(bounding_stripes.page_ids, bounding_stripes.text_ranges):
                row_locations[page_id] = []
                depth = 0
                start_loc = None
                for token in self.getPage(page_id).tokens[text_range[0]:text_range[1]]:
                    if depth > 0:
                        if token.token == list_tags[start_tag][1]:
                            depth+= 1
                        elif token.token == list_tags[start_tag][2]:
                            depth-= 1
                        if depth == 0:
                            row_locations[page_id].append((start_loc, token.token_location))
                    elif token.token == list_tags[start_tag][1]:
                        depth = 1
                        start_loc = token.token_location
            list['rows'] = row_locations
        rules = RuleSet()
        for number, list in enumerate(list_locations):
            start_tag = list['tag']
            row_locations = list['rows']
            bounding_stripes = list['stripes']
            iter_intervals = {}
            #end_intervals = {}
            row_intervals = {}
            for page_id in self._pages:
                if not row_locations[page_id]:
                    continue
                if len(row_locations[page_id]) > 1:
                    sample = random.sample(xrange(0, len(row_locations[page_id])-1), min(5, len(row_locations[page_id])-1))
                    for index in sample:
                        start_loc = row_locations[page_id][index][0]
                        next_start_loc = row_locations[page_id][index + 1][0] + 1
                        iter_intervals[(start_loc, next_start_loc, page_id)] = (start_loc, next_start_loc, page_id)
                        #end_intervals[(end_loc, next_start_loc, page_id)] = (end_loc, next_start_loc, page_id)
                #list_end = list['stripes'].text_ranges[list['stripes'].page_ids.index(page_id)][1]
                #end_intervals[(row_locations[page_id][-1][1], list_end, page_id)]=(row_locations[page_id][-1][1], list_end, page_id)
                if len(row_locations[page_id]) > 2:
                    row_sample = random.sample(row_locations[page_id][1:-1], min(5, len(row_locations[page_id])-2))
                    for row in row_sample:
                        interval = (row[0]+1, row[1],page_id)
                        row_intervals[interval] = interval

            if len(row_intervals) < 2 or len(iter_intervals) < 2:
                continue

            iter_intervals_stripes = self.learnStripes(iter_intervals)
            iter_bound_stripes = BoundingStripes()
            if not iter_intervals_stripes:
                continue
            iter_bound_stripes.bounding_stripes = (iter_intervals_stripes[0], iter_intervals_stripes[-1])
            for page_id in iter_intervals:
                iter_bound_stripes.page_ids.append(page_id)
                iter_bound_stripes.text_ranges.append((iter_intervals_stripes[0]['page_locations'][page_id]+\
                    iter_intervals_stripes[0]['tuple_size'],iter_intervals_stripes[-1]['page_locations'][page_id]))

            #end_bound_stripes = BoundingStripes()
            #end_intervals_stripes = self.learnStripes(end_intervals)
            #if not end_intervals_stripes:
            #    continue
            #end_bound_stripes.bounding_stripes = (end_intervals_stripes[0], end_intervals_stripes[-1])
            #for page_id in end_intervals:
            #    end_bound_stripes.page_ids.append(page_id)
            #    end_bound_stripes.text_ranges.append((end_intervals_stripes[0]['page_locations'][page_id]+\
            #            end_intervals_stripes[0]['tuple_size'],end_intervals_stripes[-1]['page_locations'][page_id]))

            iter_regex = self.buildRule(self.getNextLevelStripes(iter_intervals_stripes, iter_bound_stripes, 'end', False))
            #end_iter_regex = self.buildRule(self.getNextLevelStripes(end_intervals_stripes, end_bound_stripes, 'end', False))

            begin_stripes = self.getNextLevelStripes(page_stripes, bounding_stripes, 'begin', False)
            end_stripes = self.getNextLevelStripes(page_stripes, bounding_stripes, 'end', False)
            begin_regex = self.buildRule(begin_stripes)

            # assumption: relevant list tag is the last one in begin_regex
            begin_regex = (start_tag + "(?=").join(begin_regex.rsplit(start_tag, 1)) + ').*?' + escape_regex_string(
                list_tags[start_tag][1])
            end_regex = self.buildRule(end_stripes)
            if begin_regex and end_regex and iter_regex:
                rule = IterationRule('list_'+str(number), begin_regex, end_regex, iter_regex,
                                             include_end_regex=True, no_first_begin_iter_rule=True, no_last_end_iter_rule=True)
                subrules = self.learnAllRules(self.learnStripes(row_intervals), in_list=True)
                rule.set_sub_rules(subrules)
                rules.add_rule(rule)
        return rules


    def learnListMarkupsAndSubrules(self):
        random.seed(5)
        list_start_tags = list_tags.keys()

        list_locations = []
        seed_page = self.getPage(self.seed_page_id)
        loc = 0
        while loc <= len(seed_page.tokens):
            next_loc = loc + 1
            for list_start_tag in list_start_tags:
                if self.__next_loc_equals(loc, seed_page.tokens, list_start_tag):
                    logger.debug('found ' + list_start_tag + ' at ' + str(loc) + " on " + self.seed_page_id)
                    end_tag = list_tags[list_start_tag][0]
                    end = self.__find_list_span(loc + 1, seed_page.tokens, list_start_tag, end_tag)
                    if end > 0:
                        list_info = {}
                        list_info['tag'] = list_start_tag
                        list_info['pages'] = {}
                        list_info['pages'][self.seed_page_id] = {}
                        list_info['pages'][self.seed_page_id]['location'] = (loc, end)
                        list_locations.append(list_info)
                        logger.debug('found ' + end_tag + ' at ' + str(loc) + " on " + self.seed_page_id)
                        next_loc = end
            loc = next_loc

        list_locations = self.__trim_and_update_list_locations(list_locations)

        markup = {}
        all_row_rules = {}

        count = 1
        for list_location in list_locations:
            list_name = '_list' + str(count)
            count += 1

            row_page_manager = PageManager(tuple_sizes=self.tuple_sizes,
                                           write_debug_files=self._WRITE_DEBUG_FILES,
                                           do_field_prediction=self._DO_FIELD_PREDICTION,
                                           remove_bad_rules=self._REMOVE_BAD_RULES)

            for page_id in list_location['pages'].keys():
                if page_id not in markup:
                    markup[page_id] = {}
                if list_name not in markup[page_id]:
                    markup[page_id][list_name] = {}
                (start, end) = list_location['pages'][page_id]['location']
                page = self.getPage(page_id)
                list_text = page.tokens.getTokensAsString(start, end, True)
                markup[page_id][list_name]['extract'] = list_text

                rows = self.__get_list_rows(page_id, list_location)
                list_location['pages'][page_id]['rows'] = rows

                for row_info in list_location['pages'][page_id]['rows']:
                    if 'sequence' not in markup[page_id][list_name]:
                        markup[page_id][list_name]['sequence'] = []

                    row_markup = {}

                    row_markup['sequence_number'] = row_info['sequence_num']
                    (row_start, row_end) = row_info['location']

                    ## Trim off the start and end tags so we can learn things
                    row_text_offset_start = 0
                    for token in page.tokens[row_start:row_end]:
                        row_text_offset_start += 1
                        if token.token == '>':
                            break
                    row_text_offset_end = 0
                    for token in reversed(page.tokens[row_start:row_end]):
                        row_text_offset_end += 1
                        if token.token == '<':
                            break

                    row_text = page.tokens.getTokensAsString(row_start + row_text_offset_start,
                                                             row_end - row_text_offset_end, True)

                    row_markup['extract'] = row_text
                    markup[page_id][list_name]['sequence'].append(row_markup)
                    row_info['text'] = row_text

                page = self.getPage(page_id)
                rows = random.sample(list_location['pages'][page_id]['rows'], min(3, len(list_location['pages'][page_id]['rows'])))
                for row in rows:
                    (start, end) = row['location']
                    # MariaM: 09/20/17; Add '_' before seq-_num so we can extract it easily from string later
                    page_name = page_id + '_' + str(row['sequence_num'])
                    # page_name = page_id + str(row['sequence_num'])
                    row_page_manager.addPage(list_name + page_name, row['text'],
                                             False)

            row_stripes = row_page_manager.learnStripes()
            row_rules = row_page_manager.learnAllRules(row_stripes, in_list=True)
            all_row_rules[list_name] = row_rules

        return markup, all_row_rules
    
    def learnListMarkups(self):
        list_start_tags = list_tags.keys()
        
        list_locations = []
        seed_page = self.getPage(self.seed_page_id)
        loc = 0
        while loc <= len(seed_page.tokens):
            next_loc = loc + 1
            for list_start_tag in list_start_tags:
                if self.__next_loc_equals(loc, seed_page.tokens, list_start_tag):
                    logger.debug('found ' + list_start_tag + ' at ' + str(loc) + " on " + self.seed_page_id)
                    end_tag = list_tags[list_start_tag][0]
                    end = self.__find_list_span(loc+1, seed_page.tokens, list_start_tag, end_tag)
                    if end > 0:
                        list_info = {}
                        list_info['tag'] = list_start_tag
                        list_info['pages'] = {}
                        list_info['pages'][self.seed_page_id] = {}
                        list_info['pages'][self.seed_page_id]['location'] = (loc, end)
                        list_locations.append( list_info )
                        logger.debug('found ' + end_tag + ' at ' + str(loc) + " on " + self.seed_page_id)
                        next_loc = end
            loc = next_loc
        
        list_locations = self.__trim_and_update_list_locations(list_locations)
        
        markup = {}
        list_names = {}
        
        count = 1
        for list_location in list_locations:
            list_name = '_list'+str(count)
            count += 1
            
            row_page_manager = PageManager(tuple_sizes=self.tuple_sizes,
                                           write_debug_files=self._WRITE_DEBUG_FILES,
                                           do_field_prediction=self._DO_FIELD_PREDICTION,
                                           remove_bad_rules=self._REMOVE_BAD_RULES)
            
            for page_id in list_location['pages'].keys():
                if page_id not in markup:
                    markup[page_id] = {}
                if list_name not in markup[page_id]:
                    markup[page_id][list_name] = {}
                (start, end) = list_location['pages'][page_id]['location']
                page = self.getPage(page_id)
                list_text = page.tokens.getTokensAsString(start, end, True)
                markup[page_id][list_name]['extract'] = list_text
                
                rows = self.__get_list_rows(page_id, list_location)
                list_location['pages'][page_id]['rows'] = rows
                
                for row_info in list_location['pages'][page_id]['rows']:
                    if 'sequence' not in markup[page_id][list_name]:
                        markup[page_id][list_name]['sequence'] = []
                        
                    row_markup = {}
                    
                    row_markup['sequence_number'] = row_info['sequence_num']
                    (row_start, row_end) = row_info['location']
                    
                    
                    ## Trim off the start and end tags so we can learn things
                    row_text_offset_start = 0
                    for token in page.tokens[row_start:row_end]:
                        row_text_offset_start += 1
                        if token.token == '>':
                            break
                    row_text_offset_end = 0
                    for token in reversed(page.tokens[row_start:row_end]):
                        row_text_offset_end += 1
                        if token.token == '<':
                            break
                    
                    row_text = page.tokens.getTokensAsString(row_start+row_text_offset_start, row_end-row_text_offset_end, True)
                    
                    row_markup['extract'] = row_text
                    markup[page_id][list_name]['sequence'].append(row_markup)
                
                page = self.getPage(page_id)
                for row in rows:
                    (start, end) = row['location']
                    #MariaM: 09/20/17; Add '_' before seq-_num so we can extract it easily from string later
                    page_name = page_id + '_' + str(row['sequence_num'])
                    #page_name = page_id + str(row['sequence_num'])
                    row_page_manager.addPage(list_name+page_name, page.tokens.getTokensAsString(start, end, True), False)

            row_stripes = row_page_manager.learnStripes()
            row_rules = row_page_manager.learnAllRules(row_stripes, in_list=True)
            if len(row_rules.rules) > 1:
                row_markups, names = row_page_manager.rulesToMarkup(row_rules, remove_html=True)
                list_names[list_name] = names
                for markup_page in row_markups.keys():
                    # MariaM: 09/20/17; page_id doesn't have extension .html
                    seq_delim = markup_page.rfind('_')
                    page_id = markup_page[:seq_delim][len(list_name):]
                    sequence_num = markup_page[seq_delim + 1:]
                    # page_id =  markup_page.split('.html')[0][len(list_name):] + '.html'
                    # sequence_num = markup_page.split('.html')[-1]
                    for name in names:
                        if name in row_markups[markup_page] and page_id in markup:
                            # MariaM:09/19/17
                            # add rules in 'sub_rules' object
                            # markup[page_id][list_name]['sequence'][int(sequence_num)-1][name] = row_markups[markup_page][name]
                            markup[page_id][list_name]['sequence'][int(sequence_num) - 1]['sub_rules'] = row_markups[markup_page]
            else:
                #remove the html from the row items since there are no sub rules
                for page_id in markup.keys():
                    if 'sequence' in markup[page_id][list_name]:
                        for seq_item in markup[page_id][list_name]['sequence']:
                            seq_item_extract = seq_item['extract']
                            processor = RemoveHtml(seq_item_extract)
                            seq_item_extract = processor.post_process()
                            seq_item_extract = seq_item_extract.strip()
                            seq_item['extract'] = seq_item_extract
                    
                list_names[list_name] = {}
        
        return markup, list_names
    
    def __get_list_rows(self, page_id, list_location):
        row_infos = []
        (start, end) = list_location['pages'][page_id]['location']
        page = self.getPage(page_id)
        list_text = page.tokens.getTokensAsString(start, end, True)
        list_tokens = tokenize(list_text)
        
        start_tag = list_tags[list_location['tag']][1]
        end_tag = list_tags[list_location['tag']][2]
        
        loc = 0
        sequence = 1
        while loc <= len(list_tokens):
            next_loc = loc + 1
            if self.__next_loc_equals(loc, list_tokens, start_tag):
                logger.debug('found ' + start_tag + ' at ' + str(loc+start) + " on " + page_id)
                end = self.__find_list_span(loc+1, list_tokens, start_tag, end_tag)
                if end > 0:
                    row_info = {}
                    row_info['start_tag'] = start_tag
                    row_info['end_tag'] = end_tag
                    row_info['location'] = (loc+start, end+start)
                    row_info['sequence_num'] = sequence
                    sequence += 1
                    row_infos.append( row_info )
                    next_loc = end
                    logger.debug('found ' + end_tag + ' at ' + str(end+start) + " on " + page_id)
            loc = next_loc
        return row_infos

    # TODO: BA FIX FOR NEW STRIPES
    def __trim_and_update_list_locations(self, stripes, list_locations):
        new_list_locations = []
        for list_loc in list_locations:
            (start_seed_loc, end_seed_loc) = list_loc['pages'][self.seed_page_id]['location']
            start_stripe = None
            start_offset = 0
            end_stripe = None
            end_offset = 0
            locations = []
            for stripe in stripes:
                seed_page_stripe_range = range(stripe['page_locations'][self.seed_page_id], stripe['page_locations'][self.seed_page_id] + stripe['tuple_size'])
                if start_seed_loc in seed_page_stripe_range:
                    start_stripe = stripe
                    start_offset = start_seed_loc - stripe['page_locations'][self.seed_page_id]
                
                if start_stripe:
                    locations.extend(range(stripe['page_locations'][self.seed_page_id], stripe['page_locations'][self.seed_page_id]+stripe['tuple_size']))
                    
                if end_seed_loc in seed_page_stripe_range:
                    end_stripe = stripe
                    end_offset = end_seed_loc - stripe['page_locations'][self.seed_page_id]
                    break
                
            if start_stripe and end_stripe:
                continuous_items = []
                for k, g in groupby(enumerate(locations), lambda (i, x): i-x):
                    continuous_items.append(map(itemgetter(1), g))
                if len(continuous_items) > 1:
                    start_stripe_locations = start_stripe['page_locations']
                    end_stripe_locations = end_stripe['page_locations']
                    for page_id in start_stripe['page_locations'].keys():
                        list_loc['pages'][page_id] = {}
                        list_loc['pages'][page_id]['location'] = {}
                        list_loc['pages'][page_id]['location'] = (start_stripe_locations[page_id]+start_offset, end_stripe_locations[page_id]+end_offset)
                    new_list_locations.append(list_loc)
                else:
                    logger.debug('Filtered out (' + str(start_seed_loc) + ', ' + str(end_seed_loc) + ') because in the template.')
        return new_list_locations
        
    def __next_loc_equals(self, loc, seed_tokens, marker):
        tokens = tokenize(marker)
        for index in range(0, len(tokens)):
            if len(seed_tokens)-1 < (loc+index) or tokens[index].token != seed_tokens[loc+index].token:
                return False
        return True
    
    def __find_list_span(self, loc, seed_tokens, start_marker, end_marker):
        #MariaM: 092917
        #added loc>0 otherwise it goes into infinite recursion when it returns -1;
        #loc=-1 and it starts again the while and so on
        while loc > 0 and loc <= len(seed_tokens):
            next_loc = loc + 1
            if self.__next_loc_equals(loc, seed_tokens, start_marker):
                next_loc = self.__find_list_span(loc+1, seed_tokens, start_marker, end_marker)
            elif self.__next_loc_equals(loc, seed_tokens, end_marker):
                tokens = tokenize(end_marker)
                return loc+len(tokens)
            
            loc = next_loc
        return -1

    def learnStripes(self, intervals={}, markups={}):
        start_time = time.time()
        self.blacklist_locations = {}
        for page_id in markups:
            if page_id not in self.blacklist_locations:
                self.blacklist_locations[page_id] = []
            
            for markup in markups[page_id]:
                if 'starting_token_location' in markups[page_id][markup] and 'ending_token_location' in markups[page_id][markup]:
                    self.blacklist_locations[page_id].extend(range(markups[page_id][markup]['starting_token_location'], markups[page_id][markup]['ending_token_location']))
                elif 'extract' in markups[page_id][markup] and 'sequence' not in markups[page_id][markup]:
                    shortest_pairs = self.getPossibleLocations(page_id, markups[page_id][markup]['extract'])
                    if not shortest_pairs:
                        logger.info("Unable to find markup for %s on page %s: %s", markup, page_id, markups[page_id][markup]['extract'])
                    for pair in shortest_pairs:
                        self.blacklist_locations[page_id].extend(range(pair[0], pair[1]+1))
        logger.debug("--- BLACKLIST LOCATION SETUP: %s seconds ---" % (time.time() - start_time))
        
        special_blacklist_tokens = ['2014',
                                    '2015',
                                    '2016',
                                    '2017',
                                    'January',
                                    'February',
                                    'March',
                                    'April',
                                    'May',
                                    'June',
                                    'July',
                                    'August',
                                    'September',
                                    'October',
                                    'November',
                                    'December',
                                    'Jan',
                                    'Feb',
                                    'Mar',
                                    'Apr',
                                    'May',
                                    'Jun',
                                    'Jul',
                                    'Aug',
                                    'Sept',
                                    'Sep',
                                    'Oct',
                                    'Nov',
                                    'Dec'
                                    ]
        start_intervals = copy.deepcopy(intervals)
        if not start_intervals:
            for page in self._pages:
                interval = (0, len(self._pages[page].tuples_by_size[1])-1, page)
                start_intervals[interval] = interval
                # intervals[page] = [(0, len(self._pages[page].tuples_by_size[1]))]

                #ADDING special characters
                if page not in self.blacklist_locations:
                    self.blacklist_locations[page] = []
                for special_blacklist_token in special_blacklist_tokens:
                    shortest_pairs = self.getPossibleLocations(page, special_blacklist_token)
                    for pair in shortest_pairs:
                        self.blacklist_locations[page].extend(range(pair[0], pair[1]+1))
#        if not intervals:
#            for page in self._pages:
#                interval = (0, len(self._pages[page].tuples_by_size[1])-1, page)
#                intervals[interval] = interval
#                # intervals[page] = [(0, len(self._pages[page].tuples_by_size[1]))]
#
#                #ADDING special characters
#                if page not in self.blacklist_locations:
#                    self.blacklist_locations[page] = []
#                for special_blacklist_token in special_blacklist_tokens:
#                    shortest_pairs = self.getPossibleLocations(page, special_blacklist_token)
#                    for pair in shortest_pairs:
#                        self.blacklist_locations[page].extend(range(pair[0], pair[1]+1))

        start_time = time.time()
        unsorted_stripes = self.__create_stripes_recurse__(start_intervals, self.largest_tuple_size)
        logger.debug("--- RECURSIVE CREATE STRIPES: %s seconds ---" % (time.time() - start_time))

        if not unsorted_stripes:
            return []
        
        start_time = time.time()
        sorted_unmerged_stripes = []
        for item in sorted(unsorted_stripes.items()):
            sorted_unmerged_stripes.append(item[1])
        logger.debug("--- SORT STRIPES: %s seconds ---" % (time.time() - start_time))

        start_time = time.time()
        merged_stripes = sorted_unmerged_stripes
        # merged_stripes = self.__merge_stripes__(sorted_unmerged_stripes)
        logger.debug("--- MERGE STRIPES: %s seconds ---" % (time.time() - start_time))
        
        counter = 0
        for s in merged_stripes:
            s['id'] = counter
            counter = counter + 1
        
        if self._WRITE_DEBUG_FILES:
            for page in self._pages:
                output_html = DEBUG_HTML.replace('PAGE_ID', str(page))\
                    .replace('DEBUG_HTML', self.getDebugOutputBuffer(merged_stripes, page))
                self._debug_template_html[page] = output_html

        return merged_stripes
    
    def listRulesToMarkup(self, rule_set, remove_html = False):
        markup = {}
        names = {}
        
        for page_id in self._pages:
            markup[page_id] = {}
            
        for page_id in self._pages:
            page_string = self.getPage(page_id).getString()
            extraction = rule_set.extract(page_string)
            
            markup[page_id] = extraction
            for list_name in extraction:
                markup[page_id][list_name]['extract'] = ' '
                if list_name not in names:
                    names[list_name] = []
                if 'sequence' in extraction[list_name]:
                    for sequence_item in extraction[list_name]['sequence']:
                        if 'sub_rules' in sequence_item:
                            for item_name in sequence_item['sub_rules']:
                                if item_name not in names[list_name]:
                                    names[list_name].append(item_name)
        
        return markup, names
    
    def rulesToMarkup(self, rule_set, remove_html = False):
        markup = {}
        counts = {}
        for name in rule_set.names():
            counts[name] = 0

        for page_id in self._pages:
            markup[page_id] = {}
        
        names = []
        
        for page_id in self._pages:
            page_string = self.getPage(page_id).getString()
            extraction = rule_set.extract(page_string)
            for name in rule_set.names():
                if name in extraction:
                    if extraction[name]:
                        extract = extraction[name]['extract']
                        if remove_html:
                            processor = RemoveHtml(extract)
                            extract = processor.post_process()
                            extract = extract.strip()
                        if extract:
                            markup[page_id][name] = {}
                            markup[page_id][name] = extraction[name]
                            markup[page_id][name]['extract'] = extract
                            counts[name] = counts[name] + 1
                    if name not in names:
                        names.append(name)
                    
        return markup, names

    def learnAllRules(self, stripes, in_list=False, outer_slots=False, recurse=True):
        rule_set = RuleSet()
        previous_stripe = None
        count = 0
        values = []
        intervals = stripes[0]['page_locations']

        #add length zero stripes so that the beginning and end of the intervals may be considered slots
        if outer_slots:
            stripes = list(stripes)
            max_id = max([stripe['id'] for stripe in stripes])
            first_stripe = {'id': max_id+1, 'tuple_size': 0, 'stripe': '', 'level': 3, 'page_locations': OrderedDict()}
            last_stripe = {'id': max_id+2, 'tuple_size': 0, 'stripe': '', 'level': 3, 'page_locations': OrderedDict()}
            for interval in intervals:
                first_stripe['page_locations'][interval] = interval[0]
                last_stripe['page_locations'][interval] = interval[1]
            stripes.insert(0,first_stripe)
            stripes.append(last_stripe)

        for stripe in stripes:
            stripe_fragments = self.getStripeFragmentsForSlot(stripes, stripe, direction='begin')
            stripe_text = ''
            for f_stripe in stripe_fragments:
                stripe_text += f_stripe['stripe']

            if stripe_text not in cachedStopWords:
                if previous_stripe is not None:
                    num_with_visible_values = 0
                    slot_values = OrderedDict()
                    rule_stripes = BoundingStripes()
                    rule_stripes.bounding_stripes.append(previous_stripe)
                    rule_stripes.bounding_stripes.append(stripe)

                    for interval in stripe['page_locations']:
                        page_id = interval[2]
                        if interval in previous_stripe['page_locations']:
                            if previous_stripe['page_locations'][interval] + previous_stripe['tuple_size'] \
                                    != stripe['page_locations'][interval]:
                                token_locations = range(
                                    previous_stripe['page_locations'][interval] + previous_stripe['tuple_size'],
                                    stripe['page_locations'][interval])
                                value = []
                                for token_index in token_locations:
                                    if self.getPage(page_id).get_token(token_index).visible:
                                        num_with_visible_values += 1
                                    value.append(self.getPage(page_id).get_token(token_index))
                                slot_values[interval] = value
                    if num_with_visible_values > 0:
                        begin_stripes = self.getNextLevelStripes(stripes, rule_stripes, 'begin', False)
                        end_stripes = self.getNextLevelStripes(stripes, rule_stripes, 'end', False)

                        start_rule = self.buildRule(begin_stripes)
                        end_rule = self.buildRule(end_stripes)

                        strip_end_regex = ''
                        if len(end_stripes) > 0:
                            strip_end_regex_stripes = []
                            strip_end_regex_stripes.append(end_stripes[-1])
                            strip_end_regex = self.buildRule(strip_end_regex_stripes)

                        rule_name = ''
                        visible_chunk_before = ''
                        visible_chunk_after = ''
                        if not in_list:
                            # get visible chunk(s) before
                            (visible_chunk_before, visible_chunk_after) = \
                                self.__get_visible_chunk_buffers(stripes, begin_stripes, end_stripes)
                            rule_name = ''.join(visible_chunk_before.split())

                        rule_name = rule_name + str(count)
                        rule = ItemRule(rule_name, start_rule, end_rule, True, strip_end_regex)
                        if len(visible_chunk_before) > 0:
                            rule.set_visible_chunk_before(visible_chunk_before)
                        if len(visible_chunk_after) > 0:
                            rule.set_visible_chunk_after(visible_chunk_after)

                        #sub_page_manager = PageManager()
                        extraction_intervals = []
                        new_values = ''
                        interval_strings = {}
                        for interval in intervals:
                            interval_string = self.getPage(interval[2]).tokens.getTokensAsString(interval[0],interval[1], True)
                            extraction_list = rule.apply(interval_string)
                            new_values += json.dumps(flattenResult(extraction_list), sort_keys=True, indent=2,
                                                     separators=(',', ': '))
                            extraction_interval = (rule_stripes.bounding_stripes[0]['page_locations'][interval] +
                                                   rule_stripes.bounding_stripes[0]['tuple_size'],
                                                   rule_stripes.bounding_stripes[1]['page_locations'][interval],
                                                   interval[2])
                            extraction_intervals.append(extraction_interval)
                            interval_strings[extraction_interval] = interval_string
                            #if self._AUTO_LEARN_SUB_RULES:
                            #    sub_page_manager.addPage(page_id + rule_name, extraction_list['extract'])
                        if new_values not in values:
                            # Before we add it decide if we are going to sub learn
                            if self._AUTO_LEARN_SUB_RULES and len(extraction_intervals) > 0 and recurse:
                                logger.debug('sublearning for ' + previous_stripe['stripe'].encode('utf-8') + " to " +
                                    stripe['stripe'].encode('utf-8'))
                                #tf = TruffleShuffle(sub_page_manager)
                                from TreeListLearner import TreeListLearner
                                clusters = TreeListLearner.cluster_slot(self, extraction_intervals)
                                #clusters = tf.do_truffle_shuffle(algorithm='rule_size', tokens_only=True)
                                # So I think we just add these all as sub_rules for now... then we can mark them as "optional" or ??
                                if len(clusters) > 1:
                                    #print 'Rule\n'
                                    for cluster in clusters:
                                        #for interval in cluster:
                                        #    print interval_strings[interval]
                                        #print ''
                                        sub_stripes = self.learnStripes(cluster)
                                        if sub_stripes:
                                            sub_cluster_rules = self.learnAllRules(sub_stripes, outer_slots=True, recurse=False)
                                            #sub_page_manager, sub_stripes = sub_page_manager.getSubPageManager(
                                            #    clusters[cluster]['MEMBERS'])
                                            #sub_cluster_rules = sub_page_manager.learnAllRules(sub_stripes)
                                            sub_cluster_rules.removeBadRules([interval_strings[interval] for interval in cluster])
                                            #if len(intervals) == 10:
                                            #    print '\n'
                                            #    print rule_name
                                            #    print ''
                                            #    print str(sub_cluster_rules.toJson())
                                            #    print sub_stripes
                                            if len(sub_cluster_rules.rules) > 0:
                                                for num, sub_cluster_rule in enumerate(sub_cluster_rules.rules):
                                                    #sub_cluster_rule.name = clusters[cluster][
                                                    #                            'ANCHOR'] + "__" + sub_cluster_rule.name
                                                    sub_cluster_rule.name = str(num) + "__" + sub_cluster_rule.name
                                                if rule.sub_rules:
                                                    rule.sub_rules.rules.extend(sub_cluster_rules.rules)
                                                else:
                                                    rule.sub_rules = sub_cluster_rules
                                    logger.debug(str(len(clusters)) + " clusters found")

                            ## before we add it let's try to predict the field ##
                            if self._DO_FIELD_PREDICTION:
                                predict_values = []
                                for slot_value in slot_values.values():
                                    value_buffer = ''
                                    for token_value in slot_value:
                                        value_buffer = value_buffer + token_value.getTokenWithWhitespace()
                                    predict_values.append(value_buffer.strip().encode('utf8'))
                                prediction = FieldPredictor().predict([], predict_values, [], confidence_threshold=0.5)
                                if prediction:
                                    predicted_field_name = prediction[0]
                                    rule.name = predicted_field_name + str(count)
                            rule_set.add_rule(rule)
                            if self._REMOVE_BAD_RULES:
                                values.append(new_values)
                previous_stripe = stripe
            count += 1

        return rule_set

    def learnRulesFromListMarkup(self, stripes, page_markups, subrules):
        # First create a key based markup dictionary instead of page based
        page_ids = list()
        keys = list()
        for page_markup in page_markups:
            page_ids.append(page_markup)
            keys.extend(page_markups[page_markup])
        keys = list(set(keys))

        key_based_markup = {}
        for key in keys:
            if key not in key_based_markup:
                key_based_markup[key] = []
            for page_id in page_ids:
                if key in page_markups[page_id]:
                    key_based_markup[key].append({page_id: page_markups[page_id][key]})

        rule_set = RuleSet()

        for key in key_based_markup:
            # Because we have the EXACT stripes on each side, we should be able to learn these rules
            # without testing. We will only learn the "top level" rules for now and create a RuleSet
            # form them.
            pages = key_based_markup[key]

            rule_ids = set()
            for page in pages:
                for item in page:
                    if 'rule_id' in page[item]:
                        rule_ids.add(page[item]['rule_id'])

            (rule, isSequence, hasSubRules) = self.__learn_item_rule(stripes, key, pages)

            if not rule and not isSequence:
                continue
            elif isSequence:
                rule = self.__learn_sequence_rule(stripes, key, pages, rule)

            if key in subrules:
                rule.set_sub_rules(subrules[key])

            if rule:
                if len(rule_ids) == 1:
                    rule.id = rule_ids.pop()
                rule_set.add_rule(rule)

        return rule_set

    def learnRulesFromMarkup(self, stripes, page_markups):
        #First create a key based markup dictionary instead of page based
        page_ids = list()
        keys = list()
        for page_markup in page_markups:
            page_ids.append(page_markup)
            keys.extend(page_markups[page_markup])
        keys = list(set(keys))
        
        key_based_markup = {}
        for key in keys:
            if key not in key_based_markup:
                key_based_markup[key] = []
            for page_id in page_ids:
                if key in page_markups[page_id]:
                    key_based_markup[key].append({page_id:page_markups[page_id][key]})
        
        rule_set = RuleSet()
        for key in key_based_markup:
            #Because we have the EXACT stripes on each side, we should be able to learn these rules
            #without testing. We will only learn the "top level" rules for now and create a RuleSet
            #form them.
            pages = key_based_markup[key]

            rule_ids = set()
            for page in pages:
                for item in page:
                    if 'rule_id' in page[item]:
                        rule_ids.add(page[item]['rule_id'])

            (rule, isSequence, hasSubRules) = self.__learn_item_rule(stripes, key, pages)

            if not rule and not isSequence:
                continue
            elif isSequence:
                rule = self.__learn_sequence_rule(stripes, key, pages, rule)
            if hasSubRules:
                sub_rules_markup = {}
                sub_rules_page_manager = PageManager(self._WRITE_DEBUG_FILES, self.tuple_sizes)
                for page in pages:
                    page_id = page.keys()[0]
                    real_page = self.getPage(page_id)
                    sub_page_extract = rule.apply(real_page.getString())
                    
                    if sub_page_extract['extract']:
                        sub_page_id = page_id + "_sub"
                        if sub_page_id not in sub_rules_markup:
                            sub_rules_markup[sub_page_id] = {}
                        for item in page[page_id]:
                            if item not in ignore_items:
                                sub_rules_markup[sub_page_id][item] = page_markups[page_id][key][item]
                                sub_rules_page_manager.addPage(sub_page_id, sub_page_extract['extract'])
                sub_rules_stripes = sub_rules_page_manager.learnStripes(sub_rules_markup)
                sub_rules = sub_rules_page_manager.learnRulesFromMarkup(sub_rules_stripes, sub_rules_markup)

                rule.set_sub_rules(sub_rules)
            
            if rule:
                if len(rule_ids) == 1:
                    rule.id = rule_ids.pop()
                rule_set.add_rule(rule)
        return rule_set
    
    #get all visible text before and after until hit a visible slot
    
    def __get_visible_chunk_buffers(self, stripes, begin_stripes, end_stripes):
        visible_chunk_before = ''
        visible_chunk_after = ''
         
        #get visible chunk(s) before
        if begin_stripes:
            if begin_stripes[-1]['level'] == 99: # This is a "last mile stripe" so use the one before it if we have it
                if len(begin_stripes) > 1:
                    real_stripe = self.getStripeFragmentsForSlot(stripes, begin_stripes[-2])
            else:
                real_stripe = self.getStripeFragmentsForSlot(stripes, begin_stripes[-1])
            start_location = real_stripe[-1]['page_locations'].values()[0] + real_stripe[-1]['tuple_size'] - 1
            end_location = real_stripe[0]['page_locations'].values()[0]
            visible_token_count = 0

            page = self.getPage(real_stripe[0]['page_locations'].keys()[0][2])
            for i in range(start_location, end_location, -1):
                token = page.tokens[i]
                if token.visible:
                    visible_chunk_before = token.getTokenWithWhitespace() + visible_chunk_before
                    if token.token not in cachedStopWords and token.token not in string.punctuation:
                        visible_token_count = visible_token_count + 1
                elif visible_token_count > 0:
                    break
         
        #and after
        if end_stripes:
            if end_stripes[0]['level'] == 99: # This is a "last mile stripe" so use the one before it if we have it
                if len(end_stripes) > 1:
                    real_stripe = self.getStripeFragmentsForSlot(stripes, end_stripes[1])
            else:
                real_stripe = self.getStripeFragmentsForSlot(stripes, end_stripes[0])
            start_location = real_stripe[0]['page_locations'].values()[0]
            end_location = real_stripe[-1]['page_locations'].values()[0] + real_stripe[-1]['tuple_size']
            visible_token_count = 0
            page = self.getPage(real_stripe[0]['page_locations'].keys()[0][2])
            for i in range(start_location, end_location):
                token = page.tokens[i]
                if token.visible:
                    visible_chunk_after += token.getTokenWithWhitespace()
                    visible_token_count = visible_token_count + 1
                elif visible_token_count > 0:
                    break
                
        visible_chunk_before = ' '.join(visible_chunk_before.split('&nbsp;'))
        visible_chunk_after = ' '.join(visible_chunk_after.split('&nbsp;'))
        return (visible_chunk_before.strip(), visible_chunk_after.strip())
    
    def __learn_item_rule(self, stripes, key, pages):
        isSequence = False
        hasSubRules = False
        for page in pages:
            if 'sequence' in page[page.keys()[0]]:
                isSequence = True
            for item in page[page.keys()[0]]:
                if item not in ignore_items:
                    hasSubRules = True
        
        # logger.debug('Finding stripes for %s', key);
        exact_bounding_stripes = self.getExactBoundingStripesForKey(key, pages, isSequence)

        rule = None
        if exact_bounding_stripes is not None:
            begin_stripes = self.getNextLevelStripes(stripes, exact_bounding_stripes,'begin')

            #TODO: Figure out last mile if we can
            start_points = {}
            begin_goto_points = {}
            end_goto_points = {}

            #find the locations AGAIN for now... TODO: Fix this!
            for page in pages:
                page_id = page.keys()[0]

                if 'starting_token_location' in page[page_id] and 'ending_token_location' in page[page_id]:
                    begin_stripe = exact_bounding_stripes.bounding_stripes[0]
                    end_stripe = exact_bounding_stripes.bounding_stripes[1]

                    if begin_stripe['page_locations'][page_id] + begin_stripe['tuple_size'] <= page[page_id]['starting_token_location'] and \
                                            end_stripe['page_locations'][page_id] + end_stripe['tuple_size'] >= page[page_id]['ending_token_location']:
                        start_points[page_id] = begin_stripe['page_locations'][page_id] + begin_stripe['tuple_size']
                        if begin_stripe['page_locations'][page_id] + begin_stripe['tuple_size'] != page[page_id]['starting_token_location']:
                            begin_goto_points[page_id] = page[page_id]['starting_token_location'] - 1
                        if end_stripe['page_locations'][page_id] - 1 != page[page_id]['ending_token_location']:
                            end_goto_points[page_id] = page[page_id]['ending_token_location'] + 1
                elif 'extract' in page[page_id]:
                    extract = page[page_id]['extract']

                    shortest_pairs = self.getPossibleLocations(page_id, extract, False)
                    begin_stripe = exact_bounding_stripes.bounding_stripes[0]
                    end_stripe = exact_bounding_stripes.bounding_stripes[1]

                    for pair in shortest_pairs:
                        if begin_stripe['page_locations'][page_id]+begin_stripe['tuple_size'] <= pair[0] and  \
                           end_stripe['page_locations'][page_id]+end_stripe['tuple_size'] >= pair[1]:
                            start_points[page_id] = begin_stripe['page_locations'][page_id]+begin_stripe['tuple_size']
                            if begin_stripe['page_locations'][page_id]+begin_stripe['tuple_size'] != pair[0]:
                                begin_goto_points[page_id] = pair[0] - 1
                            if end_stripe['page_locations'][page_id]-1 != pair[1]:
                                end_goto_points[page_id] = pair[1] + 1
                            break
            if begin_goto_points:
                last_mile = self.__find_last_mile(start_points, begin_goto_points, 'begin')
                if last_mile:
                    logger.debug("begin last mile for %s: %s", key, last_mile['stripe'])
                    begin_stripes.append(last_mile)
                else:
                    logger.error("Could not learn begin last mile for " + key + "!")
            start_rule = self.buildRule(begin_stripes)
            end_stripes = self.getNextLevelStripes(stripes, exact_bounding_stripes, 'end')

            if end_goto_points:
                last_mile = self.__find_last_mile(start_points, end_goto_points, 'end')
                if last_mile:
                    logger.debug("end last mile for %s: %s", key, last_mile['stripe'])
                    end_stripes = []
                    end_stripes.append(last_mile)
                else:
                    logger.error("Could not learn end last mile for %s!", key)
            end_rule = self.buildRule(end_stripes)
            strip_end_regex = ''
            if len(end_stripes) > 0:
                strip_end_regex_stripes = []
                strip_end_regex_stripes.append(end_stripes[-1])
                strip_end_regex = self.buildRule(strip_end_regex_stripes)
            
            #TODO: HACK for ISI to not get HTML for extractions
            rule = ItemRule(key, start_rule, end_rule, True, strip_end_regex, None, not isSequence)
#             rule = ItemRule(key, start_rule, end_rule, True, strip_end_regex)

            (visible_chunk_before, visible_chunk_after) = \
                self.__get_visible_chunk_buffers(stripes, begin_stripes, end_stripes)
            if visible_chunk_before:
                rule.set_visible_chunk_before(visible_chunk_before)
            if visible_chunk_after:
                rule.set_visible_chunk_after(visible_chunk_after)
        return (rule, isSequence, hasSubRules)
    
    def __learn_sequence_rule(self, stripes, key, pages, item_rule):
        random.seed(7)
        if item_rule is None:
            #This is the case where we are not given the start and end of the list so we need to learn it based on number 1 and last
            # Unless the markup contains starting_token_location and ending_token_location
            
            logger.info("No item rule learned for %s attempting to learn from locations or first and last items.", key)
            for page_markup in pages:
                extract = u''
                end_extract = u''
                starting_token_location = -1
                ending_token_location = -1
                page_id = page_markup.keys()[0]
                if 'sequence' in page_markup[page_id]:
                    highest_sequence_number = 0
                    for item in page_markup[page_id]['sequence']:
                        sequence_number = item['sequence_number']
                        if sequence_number == 1:
                            highest_sequence_number = 1
                            extract = extract + item['extract']
                            if 'starting_token_location' in item:
                                starting_token_location = item['starting_token_location']
                        elif sequence_number > highest_sequence_number:
                            highest_sequence_number = sequence_number
                            end_extract = item['extract']
                            if 'ending_token_location' in item:
                                ending_token_location = item['ending_token_location']
                if starting_token_location > 0 and ending_token_location > 0:
                    page_markup[page_id]['starting_token_location'] = starting_token_location
                    page_markup[page_id]['ending_token_location'] = ending_token_location
                    #update stripes to remove these
                    list_range = range(starting_token_location, ending_token_location)

                    # TODO: BA FIX FOR NEW STRIPES
                    stripes_to_remove = []
                    for stripe in stripes:
                        if stripe['page_locations'][page_id] in list_range:
                            stripes_to_remove.append(stripe)
                    stripes = [x for x in stripes if x not in stripes_to_remove]
                if extract:
                    if highest_sequence_number == 1:
                        page_markup[page_id]['extract'] = extract
                    elif end_extract:
                        # see if there are locations for this thing... if so remove those stripes in the middle... we have to!
                        shortest_pairs = self.getPossibleLocations(page_id, extract + LONG_EXTRACTION_SEP + end_extract, False)
                        for pair in shortest_pairs:
                            list_range = range(pair[0], pair[1])

                            # TODO: BA FIX FOR NEW STRIPES
                            stripes_to_remove = []
                            for stripe in stripes:
                                if stripe['page_locations'][page_id] in list_range:
                                    stripes_to_remove.append(stripe)
                            stripes = [x for x in stripes if x not in stripes_to_remove]
                        page_markup[page_id]['extract'] = extract + LONG_EXTRACTION_SEP + end_extract

            (item_rule, isSequence, hasSubRules) = self.__learn_item_rule(stripes, key, pages)

        if item_rule is None:
            return None
        
        #adding the stuff for the beginning and end of the list.
        #now set up the sub page manager and re run to learn the iteration rule
        begin_sequence_page_manager = PageManager(self._WRITE_DEBUG_FILES, self.tuple_sizes)
        begin_sequence_starts = {}
        begin_sequence_goto_points = {}

        end_sequence_page_manager = PageManager(self._WRITE_DEBUG_FILES, self.tuple_sizes)
        end_sequence_markup = {}
        end_sequence_starts = {}
        end_sequence_goto_points = {}
        
        num_with_nothing_at_begin = 0
        num_with_sequence = 0
        num_with_nothing_at_end = 0
        
        #This is for any sub_rules in the sequence
        sub_rules_markup = {}
        sub_rules_page_manager = PageManager(self._WRITE_DEBUG_FILES, self.tuple_sizes)
        #first get the "bounding stripes" for all the items... this will help us figure out "which one" we want
#         location_finder_page_manager = PageManager(self._WRITE_DEBUG_FILES, self.largest_tuple_size)
#         
#         extracts = []
#         commmon_tokens_before_singles = []
#         commmon_tokens_after_singles = []
#         for page_markup in pages:
#             page_id = page_markup.keys()[0]
#             if 'sequence' in page_markup[page_id]:
#                 page = self.getPage(page_id)
#                 page_string = page.getString()
#                 
#                 full_sequence = item_rule.apply(page_string)
#                 location_finder_page_manager.addPage(page_id, full_sequence['extract'])
#                 for item_1 in page_markup[page_id]['sequence']:
#                     sequence_number = item_1['sequence_number']
#                     extracts.append((page_id, item_1['extract']))
#         for (page_id, extract) in extracts:
#             locations_of_item = location_finder_page_manager.getPossibleLocations(page_id, extract)
#             if len(locations_of_item) == 1:
#                 if locations_of_item[0][0] > 1:
#                     begin_location = locations_of_item[0][0]
#                     tokens_before = list(reversed(location_finder_page_manager.getPage(page_id).tokens[begin_location-5:begin_location]))
#                     commmon_tokens_before_singles.append(tokens_before)
#                 if locations_of_item[0][1] < len(location_finder_page_manager.getPage(page_id).tokens):
#                     end_location = locations_of_item[0][1]
#                     tokens_after = list(reversed(location_finder_page_manager.getPage(page_id).tokens[end_location+1:end_location+6]))
#                     commmon_tokens_after_singles.append(tokens_after)
#                     
#         string_before = ''
#         for i in range(5):
#             new_token = ''
#             num_with_new_token = 0
#             for common_token in commmon_tokens_before_singles:
#                 if new_token:
#                     if common_token[i].token == new_token:
#                         num_with_new_token += 1
#                 else:
#                     new_token = common_token[i].token
#                     num_with_new_token += 1
#             
#             if num_with_new_token == len(commmon_tokens_before_singles):
#                 string_before = new_token + string_before
#             else:
#                 break
#         print string_before
#         
#         string_after = ''
#         for i in range(5):
#             new_token = ''
#             num_with_new_token = 0
#             for common_token in commmon_tokens_after_singles:
#                 if new_token:
#                     if common_token[i].token == new_token:
#                         num_with_new_token += 1
#                 else:
#                     new_token = common_token[i].token
#                     num_with_new_token += 1
#             
#             if num_with_new_token == len(commmon_tokens_after_singles):
#                 string_after = new_token + string_after
#             else:
#                 break
#         print string_after
        for page_markup in pages:
            page_id = page_markup.keys()[0]

            if 'sequence' in page_markup[page_id]:
                logger.debug("Getting iteration rule info for %s on ... %s", key, page_id)
                num_with_sequence = num_with_sequence + 1
                
                page = self.getPage(page_id)
                page_string = page.getString()
                
                full_sequence = item_rule.apply(page_string)
                location_finder_page_manager = PageManager(self._WRITE_DEBUG_FILES, self.tuple_sizes)
                
                last_row_text = ''
                last_row_text_item1 = ''
                last_row_goto_point = 0
                highest_sequence_number = 0
                
                previous_item_location_end = 0
                #first find the item on the page
                sortedRows = sorted(page_markup[page_id]['sequence'], key=lambda k: k['sequence_number'])
                #items = random.sample(sortedRows, max(1, len(sortedRows)-2))
                items = []
                if len(sortedRows) > 0:
                    items.append(sortedRows[0])
                if len(sortedRows) > 2:
                    items.append(random.choice(sortedRows[1:-1]))
                if len(sortedRows) > 1:
                    items.append(sortedRows[-1])
                for item_1 in sortedRows:
#                 for item_1 in page_markup[page_id]['sequence']:
                    sequence_number = item_1['sequence_number']
                    if 'starting_token_location' in item_1 and 'ending_token_location' in item_1:
                        locations_of_item1 = [ [ item_1['starting_token_location'], item_1['ending_token_location'] ] ]
                        location_finder_page_manager = self
                    else:
                        location_finder_page_manager.addPage(page_id, full_sequence['extract'])
                        locations_of_item1 = location_finder_page_manager.getPossibleLocations(page_id, item_1['extract'])
                    if len(locations_of_item1) > 0:
                        item1_start_location = locations_of_item1[0][0]
                        item1_end_location = locations_of_item1[0][1]
                        if len(locations_of_item1) > 1:
                            #TODO: FIX ME! for now just take the "smallest" window that is after the last sequence items "location"
                            for location in locations_of_item1[1:]:
                                start_location = location[0]
                                end_location = location[1]
                                if end_location - start_location < item1_end_location - item1_start_location and start_location > previous_item_location_end:
                                    item1_start_location = start_location
                                    item1_end_location = end_location
                                    
                        previous_item_location_end = item1_end_location
                        #build the sub_markups and pages as we are looking through the sequence
                        if 'sub_rules' in item_1:
                            for item in item_1['sub_rules']:
                                sub_page_id = page_id+key+"_sub"+str(sequence_number)
                                if item not in ignore_items:
                                    sub_page_text = []
                                    tokens_with_detail = location_finder_page_manager.getPage(page_id).tokens
                                    for index in range(item1_start_location, item1_end_location+1):
                                        token_with_detail = tokens_with_detail[index]
                                        if token_with_detail.whitespace_text:
                                            sub_page_text.append(token_with_detail.whitespace_text)
                                        sub_page_text.append(token_with_detail.token)
                                    sub_rules_page_manager.addPage(sub_page_id, ''.join(sub_page_text))
                                    #TODO: ADD starting_token_location and end_token_location
                                    locations_of_sub_item = []
                                    if 'extract' in item_1['sub_rules'][item]:
                                        locations_of_sub_item = sub_rules_page_manager.getPossibleLocations(sub_page_id, item_1['sub_rules'][item]['extract'])
                                    if len(locations_of_sub_item) == 0:
                                        # if this is a sequence try to find the markup by the first and last item *if it has more than 1 thing
                                        sub_extract = ''
                                        sub_end_extract = ''
                                        if 'sequence' in item_1['sub_rules'][item]:
                                            highest_sequence_number = 0
                                            for sub_list_item in item_1[item]['sequence']:
                                                sub_sequence_number = sub_list_item['sequence_number']
                                                if sub_sequence_number == 1:
                                                    sub_extract = sub_extract + sub_list_item['extract']
                                                elif sub_sequence_number > highest_sequence_number:
                                                    highest_sequence_number = sub_sequence_number
                                                    sub_end_extract = sub_list_item['extract']
                                            if sub_extract and sub_end_extract:
                                                # TODO: BA check this
                                                item_1['sub_rules'][item]['extract'] = sub_extract + LONG_EXTRACTION_SEP + sub_end_extract
                                                locations_of_sub_item = sub_rules_page_manager.getPossibleLocations(sub_page_id, sub_extract + LONG_EXTRACTION_SEP + sub_end_extract, False)
                                    if len(locations_of_sub_item) > 0:
                                        sub_page_item_text = ''
                                        tokens_with_detail = sub_rules_page_manager.getPage(sub_page_id).tokens
                                        for index in range(locations_of_sub_item[0][0], locations_of_sub_item[0][1]+1):
                                            token_with_detail = tokens_with_detail[index]
                                            if token_with_detail.whitespace_text:
                                                sub_page_item_text = sub_page_item_text + token_with_detail.whitespace_text
                                            sub_page_item_text = sub_page_item_text + tokens_with_detail[index].token

                                        if sub_page_id not in sub_rules_markup:
                                            sub_rules_markup[sub_page_id] = {}
                                        sub_rules_markup[sub_page_id][item] = item_1['sub_rules'][item]
                        #find the one after it
                        item_2 = None
                        for item in page_markup[page_id]['sequence']:
                            if item['sequence_number'] == sequence_number + 1:
                                item_2 = item
                                break
                        
                        text_item1 = ''
                        tokens_with_detail = location_finder_page_manager.getPage(page_id).tokens
                        for index in range(item1_start_location, item1_end_location+1):
                            token_with_detail = tokens_with_detail[index]
                            if token_with_detail.whitespace_text:
                                text_item1 = text_item1 + token_with_detail.whitespace_text
                            text_item1 = text_item1 + tokens_with_detail[index].token
                        text_item1 = text_item1.strip()
                        if item_2:
                            if 'starting_token_location' in item_2 and 'ending_token_location' in item_2:
                                locations_of_item2 = [ [ item_2['starting_token_location'], item_2['ending_token_location'] ] ]
                            else:
                                locations_of_item2 = location_finder_page_manager.getPossibleLocations(page_id, item_2['extract'])
                            
                            if len(locations_of_item2) > 0:
                                item2_start_location = locations_of_item2[0][0]
                                item2_end_location = locations_of_item2[0][1]
                                if len(locations_of_item2) > 1:
                                    #TODO: FIX ME! for now just take the "smallest" window that is after item1's location
                                    for location in locations_of_item2[1:]:
                                        start_location = location[0]
                                        end_location = location[1]
                                        if end_location - start_location < item2_end_location - item2_start_location and start_location > item1_end_location:
                                            item2_start_location = start_location
                                            item2_end_location = end_location
                                
                                text_between = ''
                                for index in range(item1_end_location+1, item2_start_location):
                                    token_with_detail = tokens_with_detail[index]
                                    if token_with_detail.whitespace_text:
                                        text_between = text_between + token_with_detail.whitespace_text
                                    text_between = text_between + tokens_with_detail[index].token
                                    
                                text_between = text_between.replace(PAGE_BEGIN, '').replace(PAGE_END, '').strip()
                                
                                if text_between:
                                    begin_sequence_page_manager.addPage("begin"+key+page_id+str(sequence_number), text_between, False)
                                    begin_sequence_starts["begin"+key+page_id+str(sequence_number)] = 0
                                    # TODO: where does this really "end"
                                    begin_sequence_goto_points["begin"+key+page_id+str(sequence_number)] = len(tokenize(text_between))-1

                                    end_sequence_page_manager.addPage("end"+key+page_id+str(sequence_number), text_item1+text_between, False)
                                    end_sequence_markup["end"+key+page_id+str(sequence_number)] = {}
                                    end_sequence_markup["end"+key+page_id+str(sequence_number)]['item'] = {}
                                    end_sequence_markup["end"+key+page_id+str(sequence_number)]['item']['extract'] = text_item1
                                    end_sequence_starts["end"+key+page_id+str(sequence_number)] = 0
                                    end_sequence_goto_points["end"+key+page_id+str(sequence_number)] = item1_end_location - item1_start_location + 1
                        # add what is in front of this one to the begin_sequence_page_manager
                        if sequence_number == 1:
                            # TODO: BA we need to figure out these token locations to really work!!
                            list_begin_index = 0
                            if 'starting_token_location' in page_markup[page_id]:
                                list_begin_index = page_markup[page_id]['starting_token_location']

                            text_between = ''
#                             tokens_with_detail = location_finder_page_manager.getPage(page_id).tokens
                            for index in range(list_begin_index, item1_start_location):
                                token_with_detail = tokens_with_detail[index]
                                if token_with_detail.whitespace_text:
                                    text_between = text_between + token_with_detail.whitespace_text
                                text_between = text_between + tokens_with_detail[index].token
                            text_between = text_between.replace(PAGE_BEGIN, '').replace(PAGE_END, '').strip()
                            
                            if text_between:
                                begin_sequence_page_manager.addPage("begin"+key+page_id+"0", text_between, False)
                                begin_sequence_starts["begin"+key+page_id+"0"] = 0
                                # TODO: where does this really "end"
                                begin_sequence_goto_points["begin"+key+page_id+"0"] = len(tokenize(text_between))-1
                            else:
                                num_with_nothing_at_begin = num_with_nothing_at_begin + 1

                        # and add what is after this one to the end_sequence_page_manager
                        if sequence_number > highest_sequence_number:
                            highest_sequence_number = sequence_number

                            # TODO: BA we need to figure out these token locations to really work!!
                            list_end_index = len(tokens_with_detail)
                            if 'ending_token_location' in page_markup[page_id]:
                                list_end_index = page_markup[page_id]['ending_token_location']

                            text_between = ''
#                             tokens_with_detail = location_finder_page_manager.getPage(page_id).tokens
                            for index in range(item1_end_location+1, list_end_index):
                                token_with_detail = tokens_with_detail[index]
                                if token_with_detail.whitespace_text:
                                    text_between = text_between + token_with_detail.whitespace_text
                                text_between = text_between + tokens_with_detail[index].token
                            
                            last_row_text = text_between.replace(PAGE_BEGIN, '').replace(PAGE_END, '').strip()
                            last_row_text_item1 = text_item1
                            last_row_text_object = {}
                            last_row_text_object['extract'] = text_item1
                            last_row_goto_point = item1_end_location - item1_start_location + 1
                    else:
                        logger.error("Unable to find markup for sequence number %s for %s on page %s: %s", sequence_number, key, page_id, item_1['extract'])
                if last_row_text:
                    end_sequence_page_manager.addPage("end"+key+page_id+str(highest_sequence_number), last_row_text_item1+last_row_text, False)
                    end_sequence_markup["end"+key+page_id+str(highest_sequence_number)] = {}
                    end_sequence_markup["end"+key+page_id+str(highest_sequence_number)]['item'] = {}
                    end_sequence_markup["end"+key+page_id+str(highest_sequence_number)]['item']['extract'] = last_row_text_item1
                    end_sequence_starts["end"+key+page_id+str(highest_sequence_number)] = 0
                    end_sequence_goto_points["end"+key+page_id+str(highest_sequence_number)] = last_row_goto_point
                else:
                    num_with_nothing_at_end = num_with_nothing_at_end + 1
        try:
            begin_sequence_page_manager.learnStripes()
            #HACK FOR MATTs STUFF TO "Make the stripe" - Actually this way be the way we want to do it
            # Go in the stripes in reverse on the begin side until we hit a level 1
            begin_stripes = []
            for test_stripe in list(reversed(begin_sequence_page_manager._stripes)):
                begin_stripes.insert(0, test_stripe)
                if test_stripe['level'] == 1:
                    break
            begin_iter_rule = begin_sequence_page_manager.buildRule(begin_stripes)
            if not begin_iter_rule:
                logger.debug("Unable to find begin_iter_rule for %s. Attempting to learn last mile.", key)
                last_mile = begin_sequence_page_manager.__find_last_mile(begin_sequence_starts, begin_sequence_goto_points, 'begin')
                if last_mile:
                    last_mile_stripes = []
                    last_mile_stripes.append(last_mile)
                    begin_iter_rule = begin_sequence_page_manager.buildRule(last_mile_stripes)
                else:
                    logger.error("Could not learn begin last mile for %s!", key)
                    begin_iter_rule = "##ERRROR##"
        except:
            logger.debug("Unable to find begin_iter_rule for %s. Attempting to learn last mile.", key)
            last_mile = begin_sequence_page_manager.__find_last_mile(begin_sequence_starts, begin_sequence_goto_points, 'begin')
            if last_mile:
                last_mile_stripes = []
                last_mile_stripes.append(last_mile)
                begin_iter_rule = begin_sequence_page_manager.buildRule(last_mile_stripes)
            else:
                logger.error("Could not learn begin last mile for %s!", key)
                begin_iter_rule = "##ERRROR##"
        
        try:
            #HACK FOR MATTs STUFF TO "Make the stripe" - Actually this way be the way we want to do it
            # Go in the stripes in reverse on the begin side until we hit a level 1
            end_sequence_page_manager.learnStripes(end_sequence_markup)
            end_stripes = []
            for test_stripe in end_sequence_page_manager._stripes:
                end_stripes.append(test_stripe)
                if test_stripe['level'] == 1:
                    break
            
            end_iter_rule = end_sequence_page_manager.buildRule(end_stripes)
            if not end_iter_rule:
                logger.debug("Unable to find end_iter_rule for %s. Attempting to learn last mile.", key)
                last_mile = end_sequence_page_manager.__find_last_mile(end_sequence_starts, end_sequence_goto_points, 'end')
                if last_mile:
                    last_mile_stripes = []
                    last_mile_stripes.append(last_mile)
                    end_iter_rule = end_sequence_page_manager.buildRule(last_mile_stripes)
                else:
                    logger.error("Could not learn end last mile for %s!", key)   
                    end_iter_rule = "##ERRROR##"
        except:
            logger.debug("Unable to find end_iter_rule for %s. Attempting to learn last mile.", key)
            last_mile = end_sequence_page_manager.__find_last_mile(end_sequence_starts, end_sequence_goto_points, 'end')
            if last_mile:
                last_mile_stripes = []
                last_mile_stripes.append(last_mile)
                end_iter_rule = end_sequence_page_manager.buildRule(last_mile_stripes)
            else:
                logger.error("Could not learn end last mile for %s!", key)     
                end_iter_rule = "##ERRROR##"
        
        no_first_begin_iter_rule = False
        if num_with_nothing_at_begin == num_with_sequence:
            no_first_begin_iter_rule = True
            
        no_last_end_iter_rule = False
        if num_with_nothing_at_end == num_with_sequence:
            no_last_end_iter_rule = True
        
        rule = IterationRule(key, item_rule.begin_regex, item_rule.end_regex, begin_iter_rule, end_iter_rule,
                             True, item_rule.strip_end_regex, no_first_begin_iter_rule, no_last_end_iter_rule)
        
        #Now process the sub_rules if we have enough to learn anything
        if len(sub_rules_page_manager.getPageIds()) > 1:
            logger.debug("We have %s sub pages in the sequence for %s", len(sub_rules_page_manager.getPageIds()), key)
            sub_rules_stripes = sub_rules_page_manager.learnStripes(sub_rules_markup)
            sub_rules = sub_rules_page_manager.learnRulesFromMarkup(sub_rules_stripes, sub_rules_markup)
            rule.set_sub_rules(sub_rules)
        return rule
    
    def addPage(self, page_id, page, add_special_tokens = True):
        #start = time.time()
        pageObject = Page(page_id, page, self.tuple_sizes, add_special_tokens)
        #print 'page time: %f' %(time.time()-start)
        #print 'called by ', inspect.stack()[1][3]
        self._pages[pageObject._id] = pageObject
        if not self.seed_page_id:
            self.seed_page_id = page_id
        
        logger.debug('Added page_id: %s', page_id)
    
    def getPage(self, page_id):
        return self._pages[page_id]
    
    def getPageIds(self):
        return self._pages.keys()
    
    def __minimize_stripes_for_rule_recurse(self, reversed_ordered_stripes, intervals):
        if not reversed_ordered_stripes:
            return []
        new_reversed_ordered_stripes = []
        landmark_stripes = []
        while not landmark_stripes and reversed_ordered_stripes:
            reverse_candidate_stripe = reversed_ordered_stripes.pop(0)
            unique_on_pages = 0

            for interval in intervals.values():
                page = self.getPage(interval[2])
                if page.number_of_times_on_page(reverse_candidate_stripe, interval) == 1:
                    unique_on_pages += 1
                else:
                    # get out of the for loop because one is not unique
                    break
            # for page_id in reverse_candidate_stripe['page_locations'].keys():
            #     page = self.getPage(page_id)
            #     if page.number_of_times_on_page(reverse_candidate_stripe, intervals[page_id]) == 1:
            #         unique_on_pages += 1
            #     else:
            #         # get out of the for loop becuase one is not unique
            #         break

            if unique_on_pages == len(reverse_candidate_stripe['page_locations'].keys()):
                landmark_stripes.append(reverse_candidate_stripe)
            else:
                new_reversed_ordered_stripes.append(reverse_candidate_stripe)

        if landmark_stripes:
            updated_intervals = {}
            last_stripe = landmark_stripes[0]
            for interval in intervals.keys():
                updated_intervals[interval] = [last_stripe['page_locations'][interval]+last_stripe['tuple_size'],
                                               intervals[interval][1],
                                               interval[2]]
            # for page_id in self._pages.keys():
            #     updated_intervals[page_id] = [last_stripe['page_locations'][page_id]+last_stripe['tuple_size'], intervals[page_id][1]]
            landmark_stripes.extend(self.__minimize_stripes_for_rule_recurse(new_reversed_ordered_stripes,
                                                                             updated_intervals))
        else:
            logger.error("ERROR: No unique landmark in __minimize_stripes_for_rule_recurse")
        return landmark_stripes

    def buildRule(self, stripes):
        rule_regex_string = ''

        for stripe in stripes:
            val = ''

            for index in range(0, stripe['tuple_size']):
                num_with_space = 0
                for interval in stripe['page_locations']:
                    check_index_other_page = stripe['page_locations'][interval] + index
                    token = self.getPage(interval[2]).tokens[check_index_other_page]
                    if token.has_whitespace_before:
                        num_with_space = num_with_space + 1

                tok = escape_regex_string(token.token)
                if num_with_space == len(stripe['page_locations'].keys()):
                    if val:
                        num_with_space = num_with_space + 1
                        tok = "\\s+" + tok
                elif num_with_space > 0:
                    if val:
                        num_with_space = num_with_space + 1
                        tok = "\\s*" + tok
                val = val + tok

            if rule_regex_string and stripe['level'] < LAST_MILE_LEVEL:
                rule_regex_string = rule_regex_string + ".*?" + val
            else:
                rule_regex_string = rule_regex_string + val

        rule_regex_string = rule_regex_string.replace(PAGE_BEGIN + '.*?', '') \
            .replace(PAGE_BEGIN + '\\s+', '').replace(PAGE_BEGIN + '\\s*', '').replace(PAGE_BEGIN, '')

        rule_regex_string = rule_regex_string.replace('.*?' + PAGE_END, '') \
            .replace('\\s+' + PAGE_END, '').replace('\\s*' + PAGE_END, '').replace(PAGE_END, '')

        return rule_regex_string
    
    def getExactBoundingStripes(self, stripes_list):
        for stripes in stripes_list:
            if stripes.type == TYPE.EXACT:
                return stripes
    
    def getExactBoundingStripesForKey(self, key, pages, is_sequence = False):
        all_bounding_stripes = []
        for page in pages:
            shortest_pair = []
            page_id = page.keys()[0]
            if 'starting_token_location' in page[page_id] and 'ending_token_location' in page[page_id]:
                shortest_pair = [ [page[page_id]['starting_token_location'], page[page_id]['ending_token_location']] ]
            elif 'extract' in page[page_id]:
                extract = page[page_id]['extract']
                shortest_pair = self.getPossibleLocations(page_id, extract, False)
            
            bounding_stripes = self.getAllBoundingStripes(page_id, shortest_pair)
            all_bounding_stripes.append(bounding_stripes)

        if all_bounding_stripes:
            valid_bounding_stripes = self.getValidBoundingStripes(all_bounding_stripes)
            exact_bounding_stripes = self.getExactBoundingStripes(valid_bounding_stripes)
        else:
            valid_bounding_stripes = []
            exact_bounding_stripes = None
            
        if not exact_bounding_stripes:
            logger.debug('Unable to find exact bounding stripes for %s.', key)
            if len(valid_bounding_stripes) > 0:
                best_valid_bounding_stripe = valid_bounding_stripes[0]
                for valid_bounding_stripe in valid_bounding_stripes:
                    if valid_bounding_stripe.bounding_stripes[0]['level'] < best_valid_bounding_stripe.bounding_stripes[0]['level']:
                        best_valid_bounding_stripe = valid_bounding_stripe
                logger.debug('Best valid start stripe: %s', best_valid_bounding_stripe.bounding_stripes[0])
                logger.debug('Best valid end stripe: %s', best_valid_bounding_stripe.bounding_stripes[1])
                return best_valid_bounding_stripe
            else:
                logger.error('No stripes found at all for %s.', key)
        
        return exact_bounding_stripes

            
    #stripes_list contains one entry per document
    #each entry contains a list of bounding stripes; each bounding stripe is a BoundingStripes object
    #return a list of bounding stripes that are "the same" in each set
    def getValidBoundingStripes(self, stripes_list):
        #contains BoundingStripes objects
        valid_bounding_stripes = []
        first_list = stripes_list[0]
        #check each set of bounding_stripes against the other entries
        #it is valid if we find it in each of the sets
        for one_bounding_stripes in first_list:
            if stripes_list[1:]:
                for one_bounding_stripes_set in stripes_list[1:]:
                    valid_stripe = self.isValidBoundingStripe(one_bounding_stripes, one_bounding_stripes_set)
                    if valid_stripe is None:
                        break
            else:
                valid_stripe = one_bounding_stripes
            if valid_stripe is not None:
                valid_stripe.classify()
                valid_bounding_stripes.append(valid_stripe)

        return valid_bounding_stripes

    #check if bounding_stripe is in stripes_list
    def isValidBoundingStripe(self, bounding_stripe, stripes_list):
        for stripes in stripes_list:
            valid_stripe = self.isValidBoundingStripes(bounding_stripe, stripes)
            if valid_stripe is not None:
                return valid_stripe
                    
        return None

    #check if the 2 stripes are "the same"; look at location and string value
    def isValidBoundingStripes(self, stripe1, stripe2):
        #check start stripe
        start1 = stripe1.bounding_stripes[0]
        start2 = stripe2.bounding_stripes[0]
        
        #compare stripe tokens
        start_tok1 = start1['stripe']
        start_tok2 = start2['stripe']
        if start_tok1 != start_tok2:
            return None
        #compare locations in each doc
        for page_id in self._pages.keys():
            start_loc1 = start1['page_locations'][page_id]
            start_loc2 = start2['page_locations'][page_id]
            if start_loc1 != start_loc2:
                return None

        #check end stripe
        end1 = stripe1.bounding_stripes[1]
        end2 = stripe2.bounding_stripes[1]
    
        #compare stripe tokens
        end_tok1 = end1['stripe']
        end_tok2 = end2['stripe']
        if end_tok1 != end_tok2:
            return None
        #compare locations in each doc
        for page_id in self._pages.keys():
            end_loc1 = end1['page_locations'][page_id]
            end_loc2 = end2['page_locations'][page_id]
            if end_loc1 != end_loc2:
                return None

        #construct a valid stripe
        valid_stripe = BoundingStripes()
        valid_stripe.bounding_stripes = stripe1.bounding_stripes
        valid_stripe.text_ranges.extend(stripe1.text_ranges)
        valid_stripe.text_ranges.extend(stripe2.text_ranges)
        valid_stripe.page_ids.extend(stripe1.page_ids)
        valid_stripe.page_ids.extend(stripe2.page_ids)
        
        #valid_stripe = BoundingStripes()
        #valid_stripe.bounding_stripes = stripe1.bounding_stripes
        #text_ranges = []
        #page_ids = []
        #for page_id in self._pages.keys():
        #    text_ranges.append( [ start1['page_locations'][page_id], end1['page_locations'][page_id]])
        #    page_ids.append(page_id)
        #valid_stripe.text_ranges = text_ranges
        #valid_stripe.page_ids = page_ids
        
        return valid_stripe
    
    #text_location is a list of pairs [start,end]
    def getAllBoundingStripes(self, page_id, text_location):
        bounding_stripes = []
        for pair in text_location:
            one_bounding_stripes = self.getBoundingStripes(page_id, pair)
            bounding_stripes.append(one_bounding_stripes)

        return bounding_stripes

    #text_range = [start, end] of text
    def getBoundingStripes(self, stripes, page_id, text_range):

        #I want the stripes that wrap this location
        start_text = text_range[0]
        end_text = text_range[1]
        
        #if a stripe starts at the very same location as start_text, the start stripe
        # will be the one just before
        start_stripe = []
        for stripe in stripes:
            start_stripe_loc = stripe['page_locations'][page_id]
            if start_stripe == [] or start_text > start_stripe_loc:
                start_stripe = stripe
            else:
                break

        #if a stripe starts at the very same location as end_text, the end stripe
        # will be the stripe just after that
        end_stripe = []
        for stripe in stripes:
            end_stripe_loc = stripe['page_locations'][page_id]
            if end_stripe == [] or end_text >= end_stripe_loc:
                end_stripe = stripe
            else:
                end_stripe = stripe
                break

        bounding_stripes = BoundingStripes()
        bounding_stripes.bounding_stripes.append(start_stripe)
        bounding_stripes.bounding_stripes.append(end_stripe)
        bounding_stripes.text_ranges.append(text_range)
        #bounding_stripes.text_ranges.append([start_stripe_loc, end_stripe_loc])
        bounding_stripes.page_ids.append(page_id)
                    
        return bounding_stripes

    #returns the closest valid bounding stripe to the left with level = level
    #for a start-level =3 we may not have a level=2, but we may have level=1
    def getNextLevelBeginStripe(self, stripes, start_stripe_loc, level):
        #stripes are ordered by location
        left_stripe = []
        for stripe in stripes:
            left_stripe_loc = stripe['page_locations'].values()[0]
            left_stripe_level = stripe['level']
            if left_stripe_loc < start_stripe_loc and left_stripe_level <= level:
                left_stripe = stripe
            elif left_stripe_loc > start_stripe_loc:
                break

        return left_stripe

    def getNextLevelEndStripe(self, stripes, end_stripe_loc, level, start_stripe_loc):
        #stripes are ordered by location
        left_stripe = []
        for stripe in stripes:
            left_stripe_loc = stripe['page_locations'].values()[0]
            left_stripe_level = stripe['level']
            if left_stripe_loc < end_stripe_loc and left_stripe_level <= level and left_stripe_loc > start_stripe_loc:
                left_stripe = stripe
            elif left_stripe_loc > end_stripe_loc:
                break

        return left_stripe

    #stripes_obj contains a valid bounding stripe
    #returns the closest stripes to the left with level = level-1, level-2 ... up to level=1
    #type = 'begin' or 'end'
    def getNextLevelStripes(self, real_stripes, stripes_obj, type, minimize=True):
        stripes = []
        if type == 'begin':
            stripe = stripes_obj.bounding_stripes[0]
        elif type == 'end':
            stripe = stripes_obj.bounding_stripes[1]
        stripes.append(stripe)
        #it is enough if I check for next level stripe for one of the pages
        #if it is good for one page, it has to be good for all other pages because the stripes are
        #in order, so the locations of a left stripe are always less for ALL pages
        stripe_level = stripe['level']

        level_check = stripe_level
        while level_check > 0:
        # for i in range(stripe_level, 1, -1):
            if type == 'begin':
                left_stripe = self.getNextLevelBeginStripe(real_stripes, stripe['page_locations'].values()[0], level_check-1)
            elif type == 'end':
                end_stripe_loc = stripe['page_locations'].values()[0]
                start_stripe_loc = stripes_obj.bounding_stripes[0]['page_locations'].values()[0]
                left_stripe = self.getNextLevelEndStripe(real_stripes, end_stripe_loc, level_check-1, start_stripe_loc)
            if left_stripe:
                stripes.insert(0, left_stripe)
                stripe = left_stripe
                level_check = stripe['level']
            else:
                level_check -= 1

        if minimize and len(stripes) > 1:
            initial_intervals = {}
            last_stripe = stripes[-1]
            for interval in last_stripe['page_locations'].keys():
                start_loc = 0
                end_loc = last_stripe['page_locations'][interval] + last_stripe['tuple_size']
                if type == 'end':
                    start_loc = stripes_obj.bounding_stripes[0]['page_locations'][interval]
                initial_intervals[interval] = [start_loc, end_loc, interval[2]]
            # for page_id in self._pages.keys():
            #     if type == 'begin':
            #         initial_intervals[page_id] = [0, last_stripe['page_locations'][page_id]+last_stripe['tuple_size']]
            #     elif type == 'end':
            #         start_loc = stripes_obj.bounding_stripes[0]['page_locations'][page_id]
            #         initial_intervals[page_id] = [start_loc, last_stripe['page_locations'][page_id]+last_stripe['tuple_size']]
            reversed_stripes = list(reversed(stripes))
            minimized_stripes = self.__minimize_stripes_for_rule_recurse(reversed_stripes, initial_intervals)
            return minimized_stripes
        else:
            return stripes
    
    #exact_match = true will not remove html
    #a_page = a page id
    
    # replace all getPossibleLocations with this one at some point
    def getLocations(self, page_id, markup_item, exact_match=False):
        pass

    def getPossibleLocations(self, page_id, text, exact_match=False):
        #try to find it first. if we find it then just send back those locations
        locations = self.getExactLocations(page_id, text)
        if not locations:
            if LONG_EXTRACTION_SEP in text:
                locations = self.getPossibleLocationsLongExtraction(page_id, text, exact_match)
            else:
                locations = self.getPossibleLocationsContinuousTextExtraction(page_id, text, exact_match)
        
        return locations        
    
    def getExactLocations(self, page_id, text):       
        a_page = self.getPage(page_id)
        tokens = tokenize(text)
        token_list = TokenList(tokens)
        if tokens:
            token_size = len(tokens)
            if len(tokens) > self.largest_tuple_size:
                token_size = self.largest_tuple_size
            first_matches = a_page.get_location(token_size, token_list.getTokensAsString(0, token_size))
        else:
            return []
        
        poss_match_pairs = []
        for first_match in first_matches:
            #loop through the rest of the tokens to see if they are in order
            index = token_size
            for token in tokens[token_size:]:
                if token.token != a_page.get_token(first_match+index).token:
                    break
                index += 1
            if index == len(tokens):
                poss_match_pair = []
                poss_match_pair.append(first_match);
                poss_match_pair.append(first_match+index-1)
                poss_match_pairs.append(poss_match_pair)
            
        return poss_match_pairs
    
    def getPossibleLocationsLongExtraction(self, page_id, text, exact_match):
        if LONG_EXTRACTION_SEP in text:
            start_and_stop = text.split(LONG_EXTRACTION_SEP)
            text_start = start_and_stop[0]
            start_locations = self.getPossibleLocations(page_id, text_start, exact_match)
            
            text_end = start_and_stop[-1]
            end_locations = self.getPossibleLocations(page_id, text_end, exact_match)
            
        poss_match_pairs = []
        for start in start_locations:
            for end in end_locations:
                if start[1] < end[0]:
                    poss_match_pair = []
                    poss_match_pair.append(start[0])
                    poss_match_pair.append(end[1])
                    poss_match_pairs.append(poss_match_pair)
                    
        return poss_match_pairs
                
    def getPossibleLocationsContinuousTextExtraction(self, page_id, text, exact_match):
        a_page = self.getPage(page_id)
        tokens = tokenize(text)
        token_list =  TokenList(tokens)
        if exact_match == False:
            tokens = removeHtml(tokens)
        
        first_matches = []
        last_matches = []
        
        if tokens:
            first_matches = a_page.get_location(1, token_list.getTokensAsString(0, 1))
            last_matches = a_page.get_location(1, token_list.getTokensAsString(len(token_list)-1, len(token_list)))
        
        if len(first_matches) * len(last_matches) > 40:
            logger.debug("Too many matches for this markup to check!!!")
            return []
        
        poss_match_pairs = []
    
        #first_matches and last_matches are already ordered from small to large
        for first_match in first_matches:
            for last_match in last_matches:
                if last_match >= first_match:
                    poss_match_pair = []
                    poss_match_pair.append(first_match)
                    poss_match_pair.append(last_match)
                    poss_match_pairs.append(poss_match_pair)
                    #I don't want only the losest follower; I may miss matches
                    #this way (closest follower may not contain the other tokens)
                    #consider all matches at this time, they will be trimmed later
                    #break
                       
        #check if all tokens are in range and keep only pairs containing all tokens
        match_pairs = []
    
        for pair in poss_match_pairs:
            keep_pair = True
            #tokens have to be in sequence
            left_token_location = pair[0]
            for i in range(1, len(tokens)-1):
                token = tokens[i]
                #get location of token
                tok_loc = a_page.get_location(1, token)
                tok_loc = [x for x in tok_loc if x >= pair[0] and x <= pair[1]]
                #check all locations and see if we find one in range
                found_location = False
                for loc in tok_loc:
                    if left_token_location < loc and pair[1] > loc:
                        #is in range
                        found_location = True
                        left_token_location = loc
                        break
                if found_location == False:
                    keep_pair = False
                    break
            if keep_pair == True:
                match_pairs.append(pair)

        #match_pairs contain all tokens BUT the range may also contain other
        #tokens; we want to keep only the shortest pair
        shortest_length = -1
        shortest_pair = []
        for pair in match_pairs:
            length = pair[1] - pair[0]
            if shortest_length == -1 or length < shortest_length:
                shortest_length = length
                shortest_pair = []
                shortest_pair.append(pair)
            elif length == shortest_length:
                shortest_pair.append(pair)

        return shortest_pair

    # intervals = [(top_level_start, top_level_end, page_id): (start, end, page_id) , ... ]
    def __create_stripes_recurse__(self, intervals, tuple_size, level=1, seed_interval=None):

        if not self.seed_page_id:
            return []

        if not seed_interval:
            seed_interval = intervals.keys()[0]
        
        if level > self.max_level:
            self.max_level = level
        logger.debug("=== Checking Intervals (tuple_size="+str(tuple_size)+"), (level="+str(level)+"): "
                     + str(intervals))

        check_page_interval = intervals.values()[0]

        for top_level_interval in intervals:
            interval = intervals[top_level_interval]
            if interval[1]-interval[0] < check_page_interval[1]-check_page_interval[0]:
                check_page_interval = interval
        check_page_id = check_page_interval[2]

        seed_page = self._pages[check_page_id]
        stripe_candidates = OrderedDict()

        stripe_candidates_check = True
        k = check_page_interval[0]

        while stripe_candidates_check:
            if k < check_page_interval[1]:
                tuple_iter = seed_page.tuples_by_size[tuple_size][k]

                candidate_info = {'stripe': tuple_iter, 'level': level, 'tuple_size': tuple_size,
                                  'page_locations': OrderedDict()}

                # for each interval
                for top_level_interval in intervals:
                    interval = intervals[top_level_interval]
                    page_id = interval[2]
                    blacklist_locations = []
                    if page_id in self.blacklist_locations:
                        blacklist_locations = self.blacklist_locations[page_id]
                    test_page = self._pages[page_id]

                    if tuple_iter in test_page.tuple_locations_by_size[tuple_size]:
                        candidate_index = -1
                        for start_index in test_page.tuple_locations_by_size[tuple_size][tuple_iter]:
                            end_index = start_index + tuple_size
                            if interval[0] <= start_index <= interval[1] and \
                                    interval[0] <= end_index <= interval[1]:
                                if candidate_index > -1:
                                    candidate_index = -1
                                    break
                                else:
                                    candidate_index = start_index

                        if candidate_index > -1:
                            for index_check in range(candidate_index, candidate_index + tuple_size):
                                # Check that these are not in our blacklist. If ANY are then skip it!
                                if index_check in blacklist_locations:
                                    candidate_index = -1
                                    break

                        if candidate_index > -1:
                            # page_location = (interval, candidate_index)
                            # candidate_info['page_locations'].append(page_location)
                            candidate_info['page_locations'][top_level_interval] = candidate_index
                        else:
                            break
            
                if len(candidate_info['page_locations'].keys()) == len(intervals):
                    stripe_candidates[tuple_iter] = candidate_info
                    k = k + tuple_size
                else:
                    k = k + 1
                if k > check_page_interval[1]+1-tuple_size:
                    stripe_candidates_check = False
            else:
                stripe_candidates_check = False

        # ordered_stripes = self.__find_longest_conseq_subseq(stripe_candidates, {})
        ordered_stripes = self.__find_consistent_sub_sequence(stripe_candidates)
        return_stripe_info = {}

        if not ordered_stripes:
            if tuple_size > 1:
                next_tuple_size = self.tuple_sizes[self.tuple_sizes.index(tuple_size)+1]
                sub_stripes = self.__create_stripes_recurse__(intervals, next_tuple_size, level, seed_interval)
                if sub_stripes:
                    return_stripe_info.update(sub_stripes)
            elif self._FIND_LAST_MILES:
                # look for the last miles
                (begin_last_mile, begin_last_mile_tuple, begin_page_locations) =\
                    self.__find_nonunique_begin_last_mile(intervals, check_page_interval, seed_page, tuple_size)

                if begin_last_mile:
                    print '--- Found Begin Last Mile: ' + str(begin_last_mile)
                    logger.debug('--- Found Begin Last Mile: ' + str(begin_last_mile))
                    last_mile_stripe = {'stripe': begin_last_mile_tuple,
                                        'level': LAST_MILE_LEVEL,
                                        'tuple_size': len(begin_last_mile),
                                        'page_locations': begin_page_locations}
                    index = last_mile_stripe['page_locations'][seed_interval]
                    return_stripe_info[index] = last_mile_stripe

                (end_last_mile, end_last_mile_tuple, end_page_locations) =\
                    self.__find_nonunique_end_last_mile(intervals, check_page_interval, seed_page, tuple_size)

                if end_last_mile:
                    last_mile_stripe = {'stripe': end_last_mile_tuple,
                                        'level': LAST_MILE_LEVEL,
                                        'tuple_size': len(end_last_mile),
                                        'page_locations': end_page_locations}
                    index = last_mile_stripe['page_locations'][seed_interval]
                    return_stripe_info[index] = last_mile_stripe

                if begin_last_mile or end_last_mile:
                    sub_intervals = {}
                    for top_level_interval in intervals:
                        interval = intervals[top_level_interval]
                        bottom = interval[0] + len(begin_last_mile)
                        top = interval[1] - len(end_last_mile)

                        if bottom <= top:
                            sub_interval = (bottom, top, interval[2])
                            sub_intervals[top_level_interval] = sub_interval
                    if len(sub_intervals) == len(intervals):
                        sub_stripes = self.__create_stripes_recurse__(sub_intervals, 1, level, seed_interval)
                        if sub_stripes:
                            return_stripe_info.update(sub_stripes)

        else:
            previous_stripe = ''
            for stripe in ordered_stripes:
                # add it to the list to return based on the seed interval
                index = ordered_stripes[stripe]['page_locations'][seed_interval]
                return_stripe_info[index] = ordered_stripes[stripe]

                sub_intervals = {}
                sub_interval_sizes = []
                process_sub_interval = True

                # Loop through and all the sub intervals to the left of stripes
                for top_level_interval in ordered_stripes[stripe]['page_locations'].keys():
                    interval = intervals[top_level_interval]
                    if stripe == ordered_stripes.keys()[0]:
                        bottom = interval[0]
                    else:
                        bottom = ordered_stripes[previous_stripe]['page_locations'][top_level_interval] + tuple_size

                    top = ordered_stripes[stripe]['page_locations'][top_level_interval]

                    if top <= bottom:
                        process_sub_interval = False
                        break
                    sub_interval = (bottom, top, interval[2])
                    sub_intervals[top_level_interval] = sub_interval
                    sub_interval_sizes.append((top+1)-bottom)
                previous_stripe = stripe

                if process_sub_interval:
                    next_size = min(sub_interval_sizes)
                    if next_size < self.largest_tuple_size:
                        next_tuple_size = max(filter(lambda x: x <= next_size, self.tuple_sizes))
                    else:
                        next_tuple_size = self.largest_tuple_size

                    sub_stripes = self.__create_stripes_recurse__(
                        sub_intervals, next_tuple_size, level + 1, seed_interval)
                    if sub_stripes:
                        return_stripe_info.update(sub_stripes)

            #Now check the interval on the right of the last stripe
            sub_intervals = {}
            sub_interval_sizes = []
            process_sub_interval = True
            for top_level_interval in ordered_stripes[previous_stripe]['page_locations'].keys():
                interval = intervals[top_level_interval]
                bottom = ordered_stripes[previous_stripe]['page_locations'][top_level_interval] + tuple_size
                top = interval[1]
                if top <= bottom:
                    process_sub_interval = False
                    break

                sub_interval = (bottom, top, interval[2])
                sub_intervals[top_level_interval] = sub_interval
                sub_interval_sizes.append((top + 1) - bottom)
            if process_sub_interval:
                next_size = min(sub_interval_sizes)
                if next_size < self.largest_tuple_size:
                    next_tuple_size = max(filter(lambda x: x <= next_size, self.tuple_sizes))
                else:
                    next_tuple_size = self.largest_tuple_size

                sub_stripes = self.__create_stripes_recurse__(sub_intervals, next_tuple_size, level + 1, seed_interval)
                if sub_stripes:
                    return_stripe_info.update(sub_stripes)

        return return_stripe_info

    # stripe_candidates['page_locations'][interval] = location
    def __find_consistent_sub_sequence(self, stripe_candidates):
        longest_consistent_sub_sequence = list()
        inconsistent_stripes = set()

        random.seed(6)
        for i in range(1, 6):
            consistent_sub_sequence = list()
            inconsistent = False
            for stripe in stripe_candidates:
                candidate = stripe_candidates[stripe]
                if len(consistent_sub_sequence) > 0:
                    if not self.__in_order_on_all_pages(consistent_sub_sequence[0], candidate):
                        # This is the new stuff!!
                        inconsistent_stripes.add(candidate['stripe'])
                        inconsistent = True
                        choice = random.randint(0, 1)
                        if choice:
                            while len(consistent_sub_sequence) > 0:
                                if not self.__in_order_on_all_pages(consistent_sub_sequence[0], candidate):
                                    consistent_sub_sequence.pop(0)
                                else:
                                    break
                            consistent_sub_sequence.insert(0, candidate)
                    else:
                        consistent_sub_sequence.insert(0, candidate)
                else:
                    consistent_sub_sequence.insert(0, candidate)
            if len(consistent_sub_sequence) > len(longest_consistent_sub_sequence):
                longest_consistent_sub_sequence = consistent_sub_sequence
            if not inconsistent:
                break

        ordered_stripes = OrderedDict()
        for stripe_info in reversed(longest_consistent_sub_sequence):
            ordered_stripes[stripe_info['stripe']] = stripe_info

        # for stripe in stripe_candidates:
        #     print stripe_candidates[stripe]['stripe']
        #
        # print ''
        # print ''
        # print ''
        # print ''
        # print '============'
        # print ''
        # print ''
        # print ''
        # print ''
        #
        # for stripe in inconsistent_stripes:
        #     print stripe
        #
        # print ''
        # print ''
        # print ''
        # print ''
        # print '============'
        # print ''
        # print ''
        # print ''
        # print ''
        #
        # for stripe in ordered_stripes:
        #     print ordered_stripes[stripe]['stripe']

        return ordered_stripes

    def __in_order_on_all_pages(self, first_stripe, second_stripe):
        for interval in first_stripe['page_locations'].keys():
            first_location = first_stripe['page_locations'][interval]
            first_size = first_stripe['tuple_size']
            second_location = second_stripe['page_locations'][interval]
            if first_location+first_size > second_location:
                return False
        return True

        # for page_id in self._pages.keys():
        #     for location in first_stripe['page_locations'][page_id]:
        #         if location+first_stripe['tuple_size'] > second_stripe['page_locations'][page_id][location_index]:
        #             return False
        # return True

        #     if first_stripe['page_locations'][page_id]+first_stripe['tuple_size'] > \
        #                     second_stripe['page_locations'][page_id]:
        #         return False
        # return True
    
    def __find_longest_conseq_subseq(self, remaining_stripe_candidates, done_candidates):
        if not remaining_stripe_candidates:
            return {}
        group = OrderedDict()
        k, v = remaining_stripe_candidates.popitem(False)
        group[k] = v
        
        in_order_on_all = True
        first_stripe = last_stripe = k
        while in_order_on_all:
            if remaining_stripe_candidates:
                stripe = remaining_stripe_candidates.keys()[0]
                stripe_info = remaining_stripe_candidates[stripe]
                for page_id in self._pages.keys():
                    if stripe_info['page_locations'][page_id] < group[last_stripe]['page_locations'][page_id]:
                        in_order_on_all = False
                if in_order_on_all:
                    group[stripe] = stripe_info
                    remaining_stripe_candidates.popitem(False)
                    last_stripe = stripe
            else:
                in_order_on_all = False

        other_candidates = OrderedDict()
        other_candidates.update(done_candidates)
        other_candidates.update(remaining_stripe_candidates)
        
        if other_candidates:
            for stripe in other_candidates:
                stripe_info = other_candidates[stripe]
                if len(group) == 1:
                    break
                for page_id in self._pages.keys():
                    if page_id in stripe_info['page_locations']:
                        if stripe_info['page_locations'][page_id] > group[first_stripe]['page_locations'][page_id] and stripe_info['page_locations'][page_id] < group[group.keys()[-1]]['page_locations'][page_id]:
                            group.popitem()
        
        done_candidates.update(group)
        
        next_group = self.__find_longest_conseq_subseq(remaining_stripe_candidates, done_candidates)
        if len(next_group) < len(group):
            return group
        else:
            return next_group
        
    def __merge_stripes__(self, stripe_candidates):
        seed_page = self._pages[self.seed_page_id]
        merged = []
        while stripe_candidates:
            first_candidate = stripe_candidates.pop(0)
            count = 0
            if stripe_candidates:
                second_candidate = stripe_candidates[0]
                tuple_length = first_candidate['tuple_size']
                for page_id in self._pages.keys():
                    if first_candidate['page_locations'][page_id] + tuple_length > second_candidate['page_locations'][page_id]:
                        count = count + 1
            
            if count == 0:
                merged.append(first_candidate)
            elif count == len(self._pages):
                #we will "update the first candidate and remove the second
                merged_stripes = {'stripe': '','level': min(first_candidate['level'], second_candidate['level']), 'tuple_size': 0, 'page_locations': first_candidate['page_locations']}
                
                first_token_list = range(first_candidate['page_locations'][self.seed_page_id], first_candidate['page_locations'][self.seed_page_id]+first_candidate['tuple_size'])
                second_token_list = range(second_candidate['page_locations'][self.seed_page_id], second_candidate['page_locations'][self.seed_page_id]+second_candidate['tuple_size'])
                merged_token_list = sorted(list(set(second_token_list) | set(first_token_list)))
                
                merged_stripes['stripe'] = seed_page.tokens.getTokensAsString(merged_token_list[0],merged_token_list[-1]+1)
                merged_stripes['tuple_size'] = len(merged_token_list)
                
                stripe_candidates.pop(0)
                stripe_candidates.insert(0, merged_stripes)
                logger.warn("We found a merge ...")
                print "We found a merge ... "
            else:
                merged.append(first_candidate)
                stripe_candidates.pop(0)
                logger.warn("We found a merge...")
                print "We found a merge ... "
        
        return merged

    def __find_nonunique_begin_last_mile(self, intervals, check_page_interval, seed_page, tuple_size):
        begin_last_mile = list()
        begin_page_locations = OrderedDict()
        begin_last_mile_tuple = ''
        interval_offset = 0
        for k in range(check_page_interval[0], check_page_interval[1] + 1):
            tuple_iter = seed_page.tuples_by_size[tuple_size][k]

            num_same = 0
            # now check all pages for this tuple_iter
            for top_level_interval in intervals:
                interval = intervals[top_level_interval]
                page_id = interval[2]
                blacklist_locations = []
                if page_id in self.blacklist_locations:
                    blacklist_locations = self.blacklist_locations[page_id]
                test_page = self._pages[page_id]
                if interval[0] + interval_offset not in blacklist_locations:
                    if tuple_iter == test_page.tuples_by_size[tuple_size][interval[0] + interval_offset]:
                        num_same += 1
                    else:
                        break
                else:
                    break

                if top_level_interval not in begin_page_locations:
                    begin_page_locations[top_level_interval] = interval[0]

            if num_same == len(intervals):
                begin_last_mile.append(tuple_iter)
                begin_last_mile_tuple += tuple_iter
            else:
                break

            interval_offset = interval_offset + 1
        return begin_last_mile, begin_last_mile_tuple, begin_page_locations


    def __find_nonunique_end_last_mile_(self, intervals, check_page_interval, seed_page, tuple_size):
        end_last_mile = list()
        end_page_locations = OrderedDict()
        end_last_mile_tuple = ''
        return end_last_mile, end_last_mile_tuple, end_page_locations


    def __find_last_mile(self, start_indexes, goto_points, direction = 'begin'):
        # TODO: BA Figure out why goto_points is empty??
        if not goto_points:
            return None
        last_mile = self.__find_last_mile_recurse(start_indexes, goto_points, direction, [])
        if last_mile:
            seed_page_id = goto_points.keys()[0]
            tuple_string = ''
            tuple_size = len(last_mile)
            page_locations = {}
            for page_id in last_mile[0]:
                page_locations[page_id] = last_mile[0][page_id].token_location
                
            for token in last_mile:
                tuple_string += token[seed_page_id].token
            last_mile_info = {'stripe': tuple_string,'level': 99, 'tuple_size': tuple_size, 'page_locations': page_locations}
            return last_mile_info
        else:
            return None
    
    # start_indexes = 
    # last_mile = list of map of tokens
    def __find_last_mile_recurse(self, start_indexes, goto_points, direction, last_mile):
        seed_page_id = goto_points.keys()[0]
        seed_page = self._pages[seed_page_id]
        
        next_index_location_seed_page = None
        if direction == 'begin':
            next_index_location_seed_page = goto_points[seed_page_id] - len(last_mile)
            if next_index_location_seed_page < start_indexes[seed_page_id]:
                return
        elif direction == 'end':
            next_index_location_seed_page = goto_points[seed_page_id] + len(last_mile)
        
        if not next_index_location_seed_page or next_index_location_seed_page >= len(seed_page.tokens):
            return
        
        next_token = seed_page.tokens[next_index_location_seed_page]
        next_tokens = {}
        
        # Test 1
        for page_id in self._pages:
            if page_id in goto_points:
                page = self._pages[page_id];
                other_next_token_index = None
                if direction == 'begin':
                    other_next_token_index = goto_points[page_id] - len(last_mile)
                    if other_next_token_index < start_indexes[page_id]:
                        return
                elif direction == 'end':
                    other_next_token_index = goto_points[page_id] + len(last_mile)
                
                if not other_next_token_index:
                    return
                
                other_next_token = None
                if other_next_token_index < len(page.tokens):
                    other_next_token = page.tokens[other_next_token_index]
                    
                if other_next_token is None or next_token.token != other_next_token.token:
                    return None
                else:
                    next_tokens[page_id] = other_next_token
        
        # Add token to the last_mile
        if direction == 'begin':
            last_mile.insert(0, next_tokens)
        elif direction == 'end':
            last_mile.append(next_tokens)
        
        done = True
        tuple_string = ''
        for token in last_mile:
            tuple_string += token[seed_page_id].token
            
        # Test 2
        for page_id in self._pages:
            if page_id in goto_points:
                #unique on interval for this page
                page = self._pages[page_id];
                test_string = ''
                for index in range(start_indexes[page_id], goto_points[page_id]):
                    test_string += page.tokens[index].token
                if tuple_string in test_string:
                    done = False
                    break
            
        if not done:
            return self.__find_last_mile_recurse(start_indexes, goto_points, direction, last_mile)
        else:
            return last_mile

    if __name__ == '__main__':
        page_locations = {}
        page_locations['page1'] = []
        page_locations['page1'].append(11)
        page_locations['page1'].append(12)
        page_locations['page1'].append(13)
        page_locations['page1'].append(14)
        page_locations['page1'].append(15)

        page_locations['page2'] = []
        page_locations['page2'].append(21)
        page_locations['page2'].append(22)
        page_locations['page2'].append(23)
        page_locations['page2'].append(24)
        page_locations['page2'].append(25)
        print page_locations
        for page_id in page_locations.keys():
            print page_locations[page_id]
            for interval in page_locations[page_id]:
                print interval
