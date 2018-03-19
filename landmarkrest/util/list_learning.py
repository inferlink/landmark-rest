import json
import uuid
import StringIO
import csv
from TreeListLearner import TreeListLearner
from Tree import Tree
from landmark_extractor.extraction.Landmark import RuleSet, IterationRule, ItemRule
from PageManager import PageManager
from landmarkrest.data.models import Template
from landmarkrest.data.models import Page as Pg
from landmark_extractor.extraction.Landmark import flattenResult
from landmarkrest.util.util import Util
from landmarkrest.learning import TemplateFactory
import time
import codecs
from pprint import pprint

class ListLearning(object):

    def __init__(self):
        pass

    def local_list_learning(self, input_directory_path):
        page_manager = PageManager(page_dir=input_directory_path)
        stripes = page_manager.learnStripes()
        rls = self.learn_lists(page_manager, stripes)
        mkup = {}
        model_pages = {}  # this is a model-defined Page (MVC), distinct from the Page used for learning stripes
        for page_id in page_manager.getPageIds():
            page_content = page_manager.getPage(page_id).getString()
            extractions = rls.extract(page_content)
            mkup[page_id] = extractions
            model_pages[page_id] = Pg(999, page_id, page_content, '', '')
        print str(json.dumps(mkup))
        # make a template
        rules_json = rls.toJson()

        rules_list = json.loads(rules_json)
        for rule in rules_list:
            rule['id'] = str(uuid.uuid4())

        templ = Template(stripes=stripes, pages=page_manager.get_pages(),
                         rules=json.dumps(rules_list), markup=json.dumps(mkup), supervised=False)
        # then you can apply it, one page at a time, and see what the results are
        json_output_holder = {}
        for page_id in model_pages:
            res = templ.apply_template(model_pages[page_id])
            json_output_holder[page_id] = res.items()
        j_output = json.dumps(json_output_holder)

        print "JSON output:"
        print json.dumps(json_output_holder, sort_keys=True, indent=2, separators=(',', ': '))

        return j_output, rls.toJson(), mkup

    def learn_lists(self, page_manager, stripes):
        pages = page_manager.get_pages()
        page_contents = {}
        for pageid in pages:
            page_contents[pageid] = pages[pageid].getString()
        l_rules = RuleSet()  # don't call it rules so we don't shadow outer reference
        row_locations = TreeListLearner.find_lists(page_manager)
        #pprint(row_locations)
        #exit()
        #row_locations = TreeListLearner.remove_duplicate_lists(row_locations)
        list_rules = page_manager.learnListRulesFromLocations(row_locations, stripes)
        for rule in list_rules.rules:
            l_rules.add_rule(rule)
        if False:
            # generate NON-DIV List rules
            list_rules = page_manager.learnListRules(stripes)
            #(list_markup, subrules) = page_manager.learnListMarkupsAndSubrules()
            #page_manager.learnStripes(list_markup)
            #list_rules = page_manager.learnRulesFromListMarkup(list_markup, subrules)

            for rule in list_rules.rules:
                l_rules.add_rule(rule)


            # generate DIV list rules
            (paths, pg_mgr) = TreeListLearner.learn_lists_with_page_manager(page_manager, filter_tags_by='div', add_special_tokens=True)
            #(paths, pg_mgr) = TreeListLearner.learn_lists(page_contents, filter_tags_by='div', add_special_tokens=True)
            #print "PATHS"
            #print str(paths.keys())
            div_markup_init_with_dups = TreeListLearner.create_row_markups(paths, pg_mgr)
            #pprint(div_markup_init_with_dups)
            #print str(div_markup_init_with_dups)
            try:
                # I get an error on shooterswap_small; can't reproduce it
                # if the same list appears in all pages, remove it
                div_markup_init = TreeListLearner.remove_duplicate_lists(div_markup_init_with_dups)
                # div_markup_init = div_markup_init_with_dups
            except Exception:
                print "Error in remove_duplicate_lists()"
                div_markup_init = div_markup_init_with_dups
            # div_markup contains list names
            # print str(div_markup_init)
            #pprint(div_markup_init)
            #exit()
            #for page_id in div_markup_init:
            #    for list in div_markup_init[page_id]:
            #        print pg_mgr.getPage(page_id).tokens.getTokensAsString(
            #            div_markup_init[page_id][list]['ending_token_location']+5,div_markup_init[page_id][list]['ending_token_location']-5,True)
            tree = Tree()
            #div_list_rules, div_markup = tree.learn_list_rules(div_markup_init, pg_mgr)
            div_list_rules = pg_mgr.learnListRulesFromLocations(div_markup_init, stripes)
            for rule in div_list_rules.rules:
                l_rules.add_rule(rule)
            #print str(l_rules.toJson())
        return l_rules

if __name__ == '__main__':
    import sys  # only needed here
    import logging
    logging.basicConfig(level=logging.INFO)

    learner = ListLearning()
    (json_output, rules, markup) = learner.local_list_learning(sys.argv[1])

    #print str(rules)
    exit()

    output_filename = sys.argv[2]

    as_data = json.loads(json_output)

    data_to_be_processed = []

    for page_id in as_data:
        flattened_json = {'list_data': flattenResult(as_data[page_id])}
        if page_id.find("28") > -1:
            print "FLAT"
            print str(flattened_json)

        flattened_json['__PAGENAME__'] = page_id
        data_to_be_processed.append(flattened_json)

    processed_data = []
    header = []
    for item in data_to_be_processed:
        reduced_item = {}
        Util.reduce_item(reduced_item, None, item)
        header += reduced_item.keys()
        processed_data.append(reduced_item)

    header = list(set(header))
    header.sort()

    si = StringIO.StringIO()
    writer = csv.DictWriter(si, header, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    for row in processed_data:
        writer.writerow(row)
    output = si.getvalue()

    f = open(output_filename, 'w+')
    f.write(output)
    f.close()
    print "WROTE: %s" % output_filename
    #print str(json_output)
