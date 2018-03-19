from flask import Blueprint, request, jsonify, abort
from data.models import Template
from util.PageManager import PageManager
from util.TreeListLearner import TreeListLearner
from util.Tree import Tree
from data.encoder import LandmarkJSONEncoder
import json
import multiprocessing
import requests
from collections import OrderedDict
from settings import UNSUPERVISED_LEARN_LISTS
from landmark_extractor.extraction.Landmark import RuleSet
from copy import copy


learning_api = Blueprint('learning_api', __name__)


@learning_api.route('/supervised', methods=['POST'])
def supervised(pages=None, markup=None):
    if request:
        if request.method == 'POST':
            data = request.get_json(force=True)
            if 'pages' in data and 'markup' in data:
                pages = data['pages']
                markup = data['markup']
                template, debug_htmls = TemplateFactory.supervised_learning(pages, markup)
                return jsonify(template=template, debug_htmls=debug_htmls)

    elif pages and markup:
        page_map = {}
        count = 0
        for page in pages:
            count += 1
            page_id = 'page' + format(count, '02')
            page_map[page_id] = page
        template, debug_htmls = TemplateFactory.supervised_learning(page_map, markup)
        return jsonify(template=template, debug_htmls=debug_htmls)

    abort(400)


@learning_api.route('/unsupervised', methods=['POST'])
def unsupervised(pages=None):
    if request:
        if request.method == 'POST':
            data = request.get_json(force=True)

            if 'pages' in data:
                pages = data['pages']

                if 'callback' in data:
                    # if the caller provides a callback then run async
                    process = multiprocessing.Process(target=TemplateFactory.unsupervised_learning,
                                                      args=[pages, data['callback'], UNSUPERVISED_LEARN_LISTS])
                    process.start()
                    return 'ok'
                else:
                    template, debug_htmls = TemplateFactory.unsupervised_learning(pages,
                                                                                  learn_lists=UNSUPERVISED_LEARN_LISTS)
                    return jsonify(template=template, debug_htmls=debug_htmls)
    elif pages:
        page_map = {}
        count = 0
        for page in pages:
            count += 1
            page_id = 'page'+format(count, '02')
            page_map[page_id] = page
        template = TemplateFactory.unsupervised_learning(page_map)
        return template

    abort(400)


class TemplateFactory(object):

    @staticmethod
    def supervised_learning(pages, markup, debug_template=True, field_prediction=True):
        page_manager = PageManager(write_debug_files=debug_template, do_field_prediction=field_prediction)
        for page_id in pages:
            page_manager.addPage(page_id, pages[page_id])

        stripes = page_manager.learnStripes(markup)
        rules = page_manager.learnRulesFromMarkup(stripes, markup)

        rules_json = rules.toJson()
        rules_list = json.loads(rules_json, object_pairs_hook=OrderedDict)

        stringified_stripes = []
        for stripe in stripes:
            stringified_stripe = copy(stripe)
            stringified_stripe['page_locations'] = {}
            for key in stripe['page_locations']:
                stringified_stripe['page_locations'][str(key)] = stripe['page_locations'][key]
            stringified_stripes.append(stringified_stripe)

        return Template(stripes=stringified_stripe, pages=pages, rules=json.dumps(rules_list, indent=2),
                        markup=json.dumps(markup, indent=2), supervised=True), page_manager._debug_template_html

    @staticmethod
    def unsupervised_learning(pages, callback=None, learn_lists=True, debug_template=True, field_prediction=False,
                              remove_bad_rules=False):
        page_strings = []
        page_manager = PageManager(write_debug_files=debug_template, auto_learn_sub_rules=False, do_field_prediction=field_prediction,
                                   remove_bad_rules=remove_bad_rules)
        for page_id in pages:
            page_manager.addPage(page_id, pages[page_id])
            page_strings.append(pages[page_id])
        stripes = page_manager.learnStripes()

        # NON Lists
        rules = page_manager.learnAllRules(stripes)
        if remove_bad_rules:
            rules.removeBadRules(page_strings)

        if learn_lists:
            # generate NON-DIV List rules
            #(list_markup, list_names) = page_manager.learnListMarkups()
            #page_manager.learnStripes(list_markup)
            #list_rules = page_manager.learnRulesFromMarkup(list_markup)
            #list_rules = page_manager.learnListRules(stripes)
            row_locations = TreeListLearner.find_lists(page_manager)
            list_rules = page_manager.learnListRulesFromLocations(row_locations, stripes)

            for rule in list_rules.rules:
                rules.add_rule(rule)

            # generate DIV list rules
            #(paths, pg_mgr) = TreeListLearner.learn_lists(pages, filter_tags_by='div', add_special_tokens=False)
            #(paths, pg_mgr) = TreeListLearner.learn_lists_with_page_manager(page_manager, filter_tags_by='div',
            #                                                                add_special_tokens=True)
            #div_markup_init_with_dups = TreeListLearner.create_row_markups(paths, pg_mgr)
            #try:
                # I get an error on shooterswap_small; can't reproduce it
                # if the same list appears in all pages, remove it
            #    div_markup_init = TreeListLearner.remove_duplicate_lists(div_markup_init_with_dups)
                # div_markup_init = div_markup_init_with_dups
            #except Exception, e:
            #    print "Error in remove_duplicate_lists()"
            #    div_markup_init = div_markup_init_with_dups

            # div_markup contains list names
            #tree = Tree()
            #div_list_rules, div_markup = tree.learn_list_rules(div_markup_init, pg_mgr)
            #div_list_rules = pg_mgr.learnListRulesFromLocations(div_markup_init, stripes)

            #for rule in div_list_rules.rules:
            #    rules.add_rule(rule)

        rules_json = rules.toJson()
        rules_list = json.loads(rules_json, object_pairs_hook=OrderedDict)

        markup = OrderedDict()
        for page_id in page_manager.getPageIds():
            page_html = page_manager.getPage(page_id).getString()
            # json_object = json.loads(rules_json, object_pairs_hook=OrderedDict)
            rule_set = RuleSet(rules_list)
            json_extractions = rule_set.extract(page_html)
            markup[page_id] = json_extractions

        #print json.dumps(markup, sort_keys=True, indent=2, separators=(',', ': '))
        #print str(rule_set.toJson())

        stringified_stripes = []
        for stripe in stripes:
            stringified_stripe = copy(stripe)
            stringified_stripe['page_locations'] = {}
            for key in stripe['page_locations']:
                stringified_stripe['page_locations'][str(key)] = stripe['page_locations'][key]
            stringified_stripes.append(stringified_stripe)

        template = Template(stripes=stringified_stripes, pages=pages, rules=json.dumps(rules_list, indent=2),
                            markup=json.dumps(markup, indent=2),
                            supervised=False)
        if callback:
            data = dict()
            data['template'] = LandmarkJSONEncoder().default(template)
            data['debug_htmls'] = page_manager._debug_template_html
            requests.post(callback, json.dumps(data))

        return template, page_manager._debug_template_html


if __name__ == '__main__':
    import sys
    import codecs
    import os
    import traceback
    import time

    # jl_file = '/Users/bamana/tmp/eccie_lists/marketwatch_nondiv_list_test.jl'
    # rules_file = '/Users/bamana/tmp/eccie_lists/marketwatch1.json'
    # rules_object = {}
    # with codecs.open(rules_file, "r", "utf-8") as myfile:
    #     rules_object = json.loads(myfile.read())['rules']
    # rules = RuleSet(rules_object)
    # lines = []
    # with codecs.open(jl_file, "r", "utf-8") as myfile:
    #     lines = myfile.readlines()
    # for line in lines:
    #     json_object = json.loads(line)
    #     if 'raw_content' in json_object:
    #         html = json_object['raw_content']
    #         extraction_list = rules.extract(html)
    #         print json.dumps(extraction_list, separators=(',', ': '))

    pages = {}
    page_dir = '/Users/henryehrhard/Downloads/test_sites/jstor_list_pages/'
    files = [f for f in os.listdir(page_dir) if os.path.isfile(os.path.join(page_dir, f))]
    first_file = ''
    pages_size = 0
    for the_file in files:
        if the_file.startswith('.') or the_file.startswith('debug.'):
            continue

        if not first_file:
            first_file = the_file

        with codecs.open(os.path.join(page_dir, the_file), "rU", "utf-8") as myfile:
            page_str = myfile.read().encode('utf-8')
            pages[the_file] = page_str
            pages_size += len(page_str)
    print str(1.0*(pages_size/len(pages.keys()))) + ' average of ' + str(len(pages.keys())) + ' pages.'

    try:
        start_time_1 = time.time()
        learned_template, debug_htmls = \
            TemplateFactory.unsupervised_learning(pages,
                                                  learn_lists=True,
                                                  debug_template=True,
                                                  field_prediction=False,
                                                  remove_bad_rules=False)

        # rules = RuleSet(json.loads(learned_template.rules))
        # # print learned_template.rules
        #
        # for page in pages.keys():
        #     extraction_list = rules.extract(pages[page])
        #     with codecs.open(os.path.join(page_dir, 'debug.extract.'+page+'.json'), "w", "utf-8") as myfile:
        #         myfile.write(json.dumps(extraction_list, indent=2, separators=(',', ': ')))
        #
        #for page in debug_htmls.keys():
        #    with codecs.open(os.path.join(page_dir, 'debug/', 'debug.'+page), "w", "utf-8") as myfile:
        #        myfile.write(debug_htmls[page])



    except:
        traceback.print_exc()
