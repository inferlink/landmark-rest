import re
import json
import sys
from PageManager import PageManager
from Tree import Tree
import time
import TreeNode
import math
import itertools

void_tags = frozenset(['area', 'base', 'br', 'col', 'command', 'embed', 'hr', 'img', 'input', 'keygen', 'link', 'meta',
                       'param', 'source', 'track', 'wbr'])


class TreeListLearner(object):

    def __init__(self):
        self.__minEdgeWeight = 2
        self.__DEBUG = False

    @staticmethod
    def cluster_slot(pg, intervals):
        structs = pg.getVisibleTokenStructure(data_as_strings=False, data_as_tokens=True)
        sorted_structs = {}
        for interval in intervals:
            sorted_structs[interval] = []

        tree = Tree()
        forward_tree = Tree()
        for struct in structs:
            first_vis_token_loc = struct['visible_token_buffer'][0].token_location
            last_vis_token_loc = struct['visible_token_buffer'][-1].token_location
            for interval in intervals:
                if interval[2] == struct['page_id'] and last_vis_token_loc >= interval[0] and first_vis_token_loc < interval[1]:
                    visible_tokens = [a.token for a in struct['visible_token_buffer']]
                    visible_text = ''.join(visible_tokens)
                    # find html tags leading up to the current visible chunk
                    potential_toks = re.findall(r'(</?[\w\-]+.*?>)', ''.join(struct['invisible_token_buffer_before']))
                    tokens_for_path = []

                    # remove all attributes, other than class, from the html tags
                    for tok in potential_toks:
                        tokens_for_path.append(re.sub(r'\s(?!class=)[a-z\-_]+=(?:(?:\'.*?\')|(?:".*?"))', '', tok))

                    meta_struct = {
                        'visible_text': visible_text,
                        'first_vis_token_loc': first_vis_token_loc,
                        'last_vis_token_loc': last_vis_token_loc,
                        'page_id': struct['page_id'],
                        'clusters': set(),
                        'path': '<BRK>'.join(tokens_for_path),
                        'tags': list(tokens_for_path)
                    }

                    # add list of html tags, and list of visible tokens to trees
                    tokens_for_path.reverse()
                    tree.add_node_set(tokens_for_path, meta_struct)
                    forward_tree.add_node_set(visible_tokens, meta_struct)
                    sorted_structs[interval].append(meta_struct)
                    break
        count = tree.cluster_meta_data()
        forward_tree.cluster_meta_data(count)
        #if len(intervals) == 10:
        #    print '\n\ncluster all'
        #    for interval in sorted_structs:
        #        print '\n'
        #        for meta in sorted_structs[interval]:
        #            print meta['visible_text']
        #for page_id in sorted_structs:
        #    for meta in sorted_structs[page_id]:
        #        print '{:30}'.format(", ".join(sorted(meta['clusters'], key=lambda x: x.rjust(3,'0')))[:30]) + \
        #              " : " + '{:30}'.format(meta['path'][-30:]) + " : " + meta['visible_text'][:20]
        #    print ''
        #    break
        clusters = TreeListLearner.cluster_all(sorted_structs)
        #if len(intervals) == 10:
        #    print '\n\nclusters'
        #    for cluster in clusters:
        #        print '\ncluster'
        #        for interval in cluster:
        #            print ''
        #            for meta in sorted_structs[interval]:
        #                print meta['visible_text']
        return clusters

    @staticmethod
    def cluster_all(structs):
        labels = set()
        for interval in structs:
            for meta in structs[interval]:
                labels.update(meta['clusters'])

        best_cluster = {}
        most_predictions = 0
        for marker in labels:
            predictions = None
            cluster = {}
            for interval in filter(lambda x: any([marker in meta['clusters'] for meta in structs[x]]), structs.keys()):
                cluster[interval] = interval
            for interval in cluster:
                counts = {}
                for meta in structs[interval]:
                    for label in meta['clusters']:
                        if label in counts:
                            counts[label] += 1
                        else:
                            counts[label] = 1
                if predictions is None:
                    predictions = counts
                else:
                    to_remove = []
                    for prediction in predictions:
                        if prediction not in counts or predictions[prediction] != counts[prediction]:
                            to_remove.append(prediction)
                    for word in to_remove:
                        del predictions[word]
            if len(predictions) * len(cluster) > most_predictions:
                most_predictions = len(predictions) * len(cluster)
                best_cluster = cluster

        if len(best_cluster) == 0:
            for interval in structs:
                best_cluster[interval] = interval
        clusters = [best_cluster]
        remaining = {}
        for interval in structs:
            if interval not in best_cluster:
                remaining[interval] = structs[interval]
        if len(remaining) > 0:
            clusters.extend(TreeListLearner.cluster_all(remaining))
        return clusters

    @staticmethod
    def find_lists(pg):

        lists = {}
        for page_id in pg.get_pages():
            lists[page_id] = {}

        all_slot_structs = [pg.getVisibleTokenStructure(data_as_strings=False, data_as_tokens=True)]
        list_num = 0
        for slot_structs in all_slot_structs:
            tree = Tree()
            forward_tree = Tree()
            metas = dict()
            for page_id in pg.get_pages():
                metas[page_id] = []

            for struct in slot_structs:
                visible_tokens = [a.token for a in struct['visible_token_buffer']]
                visible_text = ''.join(visible_tokens)
                first_vis_token_loc = struct['visible_token_buffer'][0].token_location
                last_vis_token_loc = struct['visible_token_buffer'][-1].token_location

                #find html tags leading up to the current visible chunk
                potential_toks = re.findall(r'(</?[\w\-]+.*?>)', ''.join(struct['invisible_token_buffer_before']))
                tokens_for_path = []

                #remove all attributes, other than class, from the html tags
                for tok in potential_toks:
                    tokens_for_path.append(re.sub(r'\s(?!class=)[a-z\-_]+=(?:(?:\'.*?\')|(?:".*?"))', '', tok))


                meta_struct = {
                    'visible_text': visible_text,
                    'first_vis_token_loc': first_vis_token_loc,
                    'last_vis_token_loc': last_vis_token_loc,
                    'page_id': struct['page_id'],
                    'clusters': set(),
                    'path': '<BRK>'.join(tokens_for_path),
                    'tags': list(tokens_for_path)
                }

                #add list of html tags, and list of visible tokens to trees
                tokens_for_path.reverse()
                tree.add_node_set(tokens_for_path, meta_struct)
                forward_tree.add_node_set(visible_tokens, meta_struct)
                metas[meta_struct['page_id']].append(meta_struct)
            # print 'START TREE DISPLAY'
            # tree.display()
            # print 'END TREE DISPLAY'

            #add clusters to meta structures based on their positions in the trees
            count = tree.cluster_meta_data()
            forward_tree.cluster_meta_data(count)

            #sort meta structures by position on pages
            spans = []
            for page_id in metas:
                metas[page_id] = sorted(metas[page_id], key=lambda x: x['first_vis_token_loc'])
                span = []
                for meta in metas[page_id]:
                    span.append(meta['clusters'])
                spans.append(span)

            row_marker = TreeListLearner.find_row_marker(spans)

            #for page_id in metas:
            #    for meta in metas[page_id]:
            #        print '{:30}'.format(", ".join(sorted(meta['clusters'], key=lambda x: x.rjust(3,'0')))[:30]) + \
            #              " : " + '{:30}'.format(meta['path'][-30:]) + " : " + meta['visible_text'][:20]
            #    print ''
            #    break

            if row_marker:
                #print row_marker
                list_id = 'list_' + str(list_num)
                in_template = TreeListLearner.list_location(row_marker, metas, pg.get_pages(), lists, list_id)
                #also look for lists in the spaces before and after this list
                all_slot_structs.append(filter(
                    lambda x: x['visible_token_buffer'][-1].token_location < lists[x['page_id']][list_id][
                        'starting_token_location'], slot_structs))
                all_slot_structs.append(filter(
                    lambda x: x['visible_token_buffer'][0].token_location >= lists[x['page_id']][list_id][
                        'ending_token_location'], slot_structs))
                if in_template:
                    for page_id in lists:
                        del lists[page_id][list_id]
                else:
                    list_num += 1
        return lists

    @staticmethod
    def list_location(row_marker, metas, pages, lists, list_id):
        #print row_marker
        template_list = True
        visible_string = None
        for page in metas:
            visible_list = []
            first_elems = [None]
            marker = None
            window = []
            first_depth = None
            last_depth = None
            first_index = None
            last_index = None
            #find the highest level in the DOM between each pair of row markers and split the rows there
            for num, meta in enumerate(metas[page]):
                window.append(meta)
                if row_marker in meta['clusters']:
                    if marker:
                        visible_list.extend(window)
                        marker = meta
                        depth = 0
                        split_elem = marker
                        shallowest = 0
                        for elem in window:
                            for tag in elem['tags']:
                                match = re.search(r'<(/?)([\w\-]+).*?(/?)>', tag)
                                if match.group(2) not in void_tags and not match.group(3):
                                    if match.group(1):
                                        depth -= 1
                                        if depth < shallowest:
                                            shallowest = depth
                                            split_elem = elem
                                    else:
                                        depth += 1
                        if first_depth is None:
                            first_depth = shallowest
                        last_depth = shallowest - depth
                        last_index = num
                        first_elems.append(split_elem)
                    else:
                        first_index = num
                        marker = meta
                    window = []

            if visible_string is None:
                visible_string = ''.join([meta['visible_text'] for meta in visible_list])
            elif template_list and visible_string != ''.join([meta['visible_text'] for meta in visible_list]):
                template_list = False

            #find the beginning of the first row at the same DOM level that splits the first and second rows
            if first_depth:
                depth = 0
                done = False
                for meta in metas[page][first_index::-1]:
                    for tag in reversed(meta['tags']):
                        match = re.search(r'<(/?)([\w\-]+).*?(/?)>', tag)
                        if match.group(2) not in void_tags and not match.group(3):
                            if match.group(1):
                                depth += 1
                            else:
                                depth -= 1
                                if depth <= first_depth:
                                    first_elems[0] = meta
                                    done = True
                                    break
                    if done:
                        break
                if not done:
                    first_elems[0] = metas[page][0]

                #find the end of the last row at the same DOM level that splits the last and second to last rows
                depth = 0
                done = False
                prev_meta = metas[page][last_index]
                for meta in metas[page][last_index+1:]:
                    for tag in meta['tags']:
                        match = re.search(r'<(/?)([\w\-]+).*?(/?)>', tag)
                        if match.group(2) not in void_tags and not match.group(3):
                            if match.group(1):
                                depth -= 1
                                if depth <= last_depth:
                                    first_elems.append(prev_meta)
                                    done = True
                                    break
                            else:
                                depth += 1
                    if done:
                        break
                    prev_meta = meta
                if not done:
                    first_elems.append(prev_meta)

                prev_elem = None
                lists[page][list_id] = {'sequence':[]}
                for num, elem in enumerate(first_elems):
                    #print elem['visible_text']
                    if prev_elem:
                        if num == len(first_elems) - 1:
                            row_data = {
                                'extract': pages[page].tokens.getTokensAsString(prev_elem['first_vis_token_loc'],
                                                                                elem['last_vis_token_loc']+1, True),
                                'sequence_number': num,
                                'starting_token_location': prev_elem['first_vis_token_loc']
                            }
                        else:
                            row_data = {
                                'extract': pages[page].tokens.getTokensAsString(prev_elem['first_vis_token_loc'],elem['first_vis_token_loc'], True),
                                'sequence_number': num,
                                'starting_token_location': prev_elem['first_vis_token_loc']
                            }
                        lists[page][list_id]['sequence'].append(row_data)
                        lists[page][list_id]['ending_token_location'] = elem['last_vis_token_loc']+1

                    prev_elem = elem
                lists[page][list_id]['starting_token_location'] = lists[page][list_id]['sequence'][0]['starting_token_location']
            else:
                lists[page][list_id] = {}
                lists[page][list_id]['starting_token_location'] = marker['first_vis_token_loc']
                lists[page][list_id]['ending_token_location'] = marker['last_vis_token_loc']+1
        return template_list

    @staticmethod
    def find_row_marker(spans):

        #only look for markers that appear on every page
        terminals = set()
        first = True
        for span in spans:
            span_terminals = set()
            for element in span:
                span_terminals.update(element)
            if first:
                terminals = span_terminals
                first = False
            else:
                terminals.intersection_update(span_terminals)

        best_marker = None
        #terminals = sorted(terminals, key=lambda x: x.rjust(3, '0'))
        most_predictions = 0
        for row_marker in terminals:
            predictions = None
            #get the strings that appear between each pair of row markers
            folds = []
            for span in spans:
                fold = None
                count = 0
                for word in span:
                    if row_marker in word:
                        if fold:
                            folds.append(fold)
                            count+=1
                        fold = [word]
                    elif fold:
                        fold.append(word)
            #if there are not a certain number of folds on average, it's probably not a list
            if len(folds) > 1.25 * len(spans):
                #find the items which we can predict exactly how many times they appear between each pair of markers
                for fold in folds:
                    counts = dict()
                    for word in fold:
                        for label in word:
                            if label in counts:
                                counts[label] += 1
                            elif label != row_marker:
                                counts[label] = 1
                    if predictions is None:
                        predictions = counts
                    else:
                        to_remove = []
                        for prediction in predictions:
                            if prediction not in counts or predictions[prediction] != counts[prediction]:
                                to_remove.append(prediction)
                        for word in to_remove:
                            del predictions[word]
                if len(predictions)*len(folds) > most_predictions:
                    most_predictions = len(predictions)*len(folds)
                    best_marker = row_marker
            #if predictions:
            #    print row_marker
            #    print len(predictions)*len(folds)
            #    print len(predictions)
            #    print len(folds)
            #    for prediction in predictions:
            #        print prediction + ": " + str(predictions[prediction])
            #    print ''
        return best_marker

    @staticmethod
    def learn_lists_with_page_manager(pg, filter_tags_by=None, add_special_tokens=True):
        tree = Tree()
        structure_data = pg.getVisibleTokenStructure(data_as_strings=False, data_as_tokens=True)
        # want to keep track of the first token location for visible text and the last token location for visible text
        # we can do this pretty easily as meta-data for our node
        for struct in structure_data:
            visible_text = ''.join([a.token for a in struct['visible_token_buffer']])
            first_vis_token_loc = struct['visible_token_buffer'][0].token_location
            last_vis_token_loc = struct['visible_token_buffer'][-1].token_location

            if filter_tags_by is None:
                tokens_for_path = struct['invisible_token_buffer_before']
            else:
                if filter_tags_by == 'space_punct':
                    token_html = ''.join(struct['invisible_token_buffer_before']).replace("><", "> <")
                    # simple way... replace all the punct we care about (=, ., /, :) with space
                    punct_to_split = ["/", "=", "-", ":"]
                    for p in punct_to_split:
                        token_html = token_html.replace(p, " ")
                    print token_html
                    tokens_for_path = token_html.split()
                else:
                    potential_toks = re.findall("(<.+?>)", ''.join(struct['invisible_token_buffer_before']))
                    tokens_for_path = [a for a in potential_toks if a.startswith("<" + filter_tags_by)]

            meta_struct = {
                'visible_text': visible_text,
                'first_vis_token_loc': first_vis_token_loc,
                'last_vis_token_loc': last_vis_token_loc,
                'page_id': struct['page_id']
            }

            tokens_for_path.reverse()
            # how do we want to process these tokens right now? Can consider all the tokens
            tree.add_node_set(tokens_for_path, meta_struct)
        tree.prune()
        paths = tree.valid_paths()
        for path in paths:
            print path.replace(tree.getPathDelimeter(), '')
            for m in paths[path]:
                if m['page_id'] == 'tmp1.html':
                    print "\t" + m['visible_text'] + " " + m['page_id'] + " " + str(m['first_vis_token_loc'])
        # print 'START TREE DISPLAY'
        # tree.display()
        # print 'END TREE DISPLAY'
        return paths, pg

    @staticmethod
    def learn_lists(contents, filter_tags_by=None, add_special_tokens=True):
        tree = Tree()
        pg = PageManager()
        for pageid in contents:
            content = contents[pageid]
            pg.addPage(pageid, content, add_special_tokens)
        structure_data = pg.getVisibleTokenStructure(data_as_strings=False, data_as_tokens=True)

        # want to keep track of the first token location for visible text and the last token location for visible text
        # we can do this pretty easily as meta-data for our node
        for struct in structure_data:
            visible_text = ''.join([a.token for a in struct['visible_token_buffer']])
            first_vis_token_loc = struct['visible_token_buffer'][0].token_location
            last_vis_token_loc = struct['visible_token_buffer'][-1].token_location

            if filter_tags_by is None:
                tokens_for_path = struct['invisible_token_buffer_before']
            else:
                if filter_tags_by == 'space_punct':
                    token_html = ''.join(struct['invisible_token_buffer_before']).replace("><", "> <")
                    # simple way... replace all the punct we care about (=, ., /, :) with space
                    punct_to_split = ["/", "=", "-", ":"]
                    for p in punct_to_split:
                        token_html = token_html.replace(p, " ")
                    print token_html
                    tokens_for_path = token_html.split()
                else:
                    potential_toks = re.findall("(<.+?>)", ''.join(struct['invisible_token_buffer_before']))
                    tokens_for_path = [a for a in potential_toks if a.startswith("<" + filter_tags_by)]

            meta_struct = {
                'visible_text': visible_text,
                'first_vis_token_loc': first_vis_token_loc,
                'last_vis_token_loc': last_vis_token_loc,
                'page_id': struct['page_id']
            }

            tokens_for_path.reverse()
            # how do we want to process these tokens right now? Can consider all the tokens
            tree.add_node_set(tokens_for_path, meta_struct)

        tree.prune()
        paths = tree.valid_paths()
        for path in paths:
            print path.replace(tree.getPathDelimeter(), '')
            for m in paths[path]:
                if m['page_id'] == 'tmp1.html':
                    print "\t" + m['visible_text']+" "+m['page_id']+" "+str(m['first_vis_token_loc'])
        #print 'START TREE DISPLAY'
        #tree.display()
        #print 'END TREE DISPLAY'

        return paths, pg

    """
        Given the rows we extract, separate them into clusters where you have overlapping rows or not.
        This is the first step to finding interleaving...

        Once we find the interleaving, we merge them in (via common parts of the paths), and create
        the lists.

        From that, we make markup and that's what we give back
    """

    #takes advantage of new tokens
    @staticmethod
    def create_row_markups(valid_rows, page_manager):
        valid_rows_by_page = {}
        for path in valid_rows:
            for row in valid_rows[path]:
                pg = row['page_id']
                if pg not in valid_rows_by_page:
                    valid_rows_by_page[pg] = {}
                if path not in valid_rows_by_page[pg]:
                    valid_rows_by_page[pg][path] = []
                valid_rows_by_page[pg][path].append(row)

        markup_by_page = {}
        for page in valid_rows_by_page:
            all_tokens_list = page_manager.getPage(page).tokens

            markup_by_page[page] = {}
            valid_rows_for_page = valid_rows_by_page[page]
            # print "VALID ROWS FOR: %s" % page
            # print str(valid_rows_for_page)

            earliest_latest_row_locations = {}
            for path in valid_rows_for_page:  # the path defines the row...
                earliest = -1
                latest = -1

                for row in valid_rows_for_page[path]:
                    s_loc = row['first_vis_token_loc']
                    e_loc = row['last_vis_token_loc']
                    if earliest == -1:
                        earliest = row['first_vis_token_loc']
                        latest = row['last_vis_token_loc']
                        continue

                    if s_loc < earliest:
                        earliest = s_loc
                    if e_loc > latest:
                        latest = e_loc

                earliest_latest_row_locations[path] = (earliest, latest)

            overlaps = []
            for pth in sorted(earliest_latest_row_locations.keys(), key= lambda x: earliest_latest_row_locations[x][0]):
                begin = earliest_latest_row_locations[pth][0]
                end = earliest_latest_row_locations[pth][1]
                if begin == -1 or end == -1:  # ill defined locations
                    continue
                if len(overlaps) == 0:  # first guy...
                    overlaps.append([pth])
                    continue

                overlap_clust = -1
                for clust_id in range(len(overlaps)):
                    cluster = overlaps[clust_id]
                    for cpath in cluster:  # could probably just find min and max of cluster and check w/ that, but easier for now...
                        p_begin = earliest_latest_row_locations[cpath][0]
                        p_end = earliest_latest_row_locations[cpath][1]

                        #  now, see if there is not  overlap...
                        if p_end < begin or p_begin > end:
                            continue
                        overlap_clust = clust_id

                if overlap_clust == -1:
                    overlaps.append([pth])
                else:
                    overlaps[overlap_clust].append(pth)

            #print "OVERLAPS"
            #print str(overlaps)
            #print page
            for clust in overlaps:
                # print "===oo00 CLUSTER 00oo==="
                # print clust
                path_for_start = ""
                # left most, largest row is the beginning, so use that one as A's'
                rows_start_location = 999999999999
                rows_end_location = 0

                # first, find the member with the most rows
                max_rows = max([len(valid_rows_for_page[member]) for member in clust])

                # Ok, so the HTML between rows could have been messed up before bc we didn't know that these were
                # overlapping lists. For instance, the first row could be alone and now it's merged, so let's remake
                # the html between...

                by_location_tuples = []  # it will be (start, end, path) just to make it super easy to build the markup
                # then once we have this filled in, and we know which path demarcates each row, we simply sort
                # then iterate thru making up the rows...
                for member in clust:
                    num_rows = len(
                        valid_rows_for_page[member])  # its ok that its combined across pages... bc aggregate number
                    # print "\t--> (%d, %d): %d" % (earliest_latest_row_locations[member][0],
                    #                               earliest_latest_row_locations[member][1], num_rows)
                    # print "\t\t PATH: " + member
                    #if num_rows > 2:
                    #    print member
                    #    print num_rows
                    #    print earliest_latest_row_locations[member][0]
                    for b in valid_rows_for_page[member]:
                        by_location_tuples.append(
                            (b['first_vis_token_loc'], b['last_vis_token_loc'], member, b['page_id']))
                        # print "\t\t\t%s %s %s %s" % (str(b['first_vis_token_loc']), b['visible_text'],
                        #                              str(b['last_vis_token_loc']), b['page_id'])

                    if num_rows == max_rows:
                        if earliest_latest_row_locations[member][0] < rows_start_location:
                            rows_start_location = earliest_latest_row_locations[member][0]
                            path_for_start = member
                        if earliest_latest_row_locations[member][1] > rows_end_location:
                            # TODO: BA I think we need to extend this "if it still has overlap with the others??"
                            rows_end_location = earliest_latest_row_locations[member][1]
                print ">> Row starts at: %d and ends at %d (%s) " % (
                rows_start_location, rows_end_location, path_for_start)
                sorted_loc_triples = sorted(by_location_tuples)
                # print "SORTED LOCATION TRIPLES"
                # print str(by_location_tuples)

                # MariaM: 012017
                prev_start_tags = []

                # Matt M: 041317
                # now we know which path is the "start" and where each one begins and ends, so let's make the structure
                # first we want to find all entries of path_for_start
                # MariaM: 092717
                # this works with shooterswap, but will not work for jair where we have rows <div> <div odd>
                rows = [(tpl[0], tpl[3]) for tpl in sorted_loc_triples if tpl[2] == path_for_start]
                # Below works with jair, but only with jair; all tuples in sorted_loc_triples are between
                # rows_start_location and rows_end_location so below test will choose all tuples
                # rows = [(tpl[0], tpl[3]) for tpl in sorted_loc_triples
                #         if tpl[0] >= rows_start_location and tpl[1] <= rows_end_location]

                for idx in range(len(rows)):
                    lc = rows[idx][0]
                    if idx < len(rows) - 1:
                        lc_next = rows[idx + 1][0]
                        end = lc_next - 1  # we go till right before this guy
                    else:
                        end = rows_end_location
                        # MariaM: 012017
                        # this is the last row; extend it to the first start tag that did not appear
                        # in previous rows
                        prev_start_tags = list(set(prev_start_tags))
                        token_index = end
                        for token in all_tokens_list[end+1:]:
                            token_index += 1
                            tag = re.match("<[a-z]+", token.token)
                            if tag is not None and tag.group(0) not in prev_start_tags:
                                # we stop here with the last row;
                                end = token_index - 1
                                break
                    # TODO: BA This is where we "strip" the invisible stuff from the end
                    row_text_offset_end = 0
                    for token in reversed(all_tokens_list[lc:end + 1]):
                        if token.visible:
                            break
                        row_text_offset_end += 1
                        # if token.token == '<':
                        #     break
                    end = end - row_text_offset_end
                    # TODO: BA And we add some stuff to the end of the last one because it causes some issues...
                    # if idx == len(rows)-1:
                    #     row_text_offset_last = 0
                    #     for token in all_tokens_list[end + 1:]:
                    #         if token.visible:
                    #             break
                    #         row_text_offset_last += 1
                    #         # if token.token == '<':
                    #         #     break
                    #     end = end + row_text_offset_last


                    # get the location info between...
                    # from all tokens, get all the tokens between and get the string
                    # then add the markup (include the sequency number)
                    markup_value = all_tokens_list.getTokensAsString(lc, end + 1, whitespace=True)

                    # MariaM: 012017
                    if idx < len(rows) - 1:
                        # collect all start tags
                        seen_etags = [s for s in re.findall("<[a-z]+", markup_value)]
                        prev_start_tags.extend(seen_etags)

                    markup_data = {
                        'extract': markup_value,
                        'sequence_number': idx + 1,
                        'starting_token_location': lc,
                        'ending_token_location': end
                    }
                    # print "%s: (%s)" % (page, str(markup_data))

                    if path_for_start not in markup_by_page[page]:
                        markup_by_page[page][path_for_start] = {
                            'sequence': []
                        }

                    markup_by_page[page][path_for_start]['sequence'].append(markup_data)
        for page in markup_by_page:

            for path_for_start in markup_by_page[page]:
                min_location = 9999999999
                max_location = -1
                for idx in range(len(markup_by_page[page][path_for_start]['sequence'])):
                    if markup_by_page[page][path_for_start]['sequence'][idx]['starting_token_location'] < min_location:
                        min_location = markup_by_page[page][path_for_start]['sequence'][idx]['starting_token_location']
                    if markup_by_page[page][path_for_start]['sequence'][idx]['ending_token_location'] > max_location:
                        max_location = markup_by_page[page][path_for_start]['sequence'][idx]['ending_token_location']
                markup_by_page[page][path_for_start]['starting_token_location'] = min_location
                markup_by_page[page][path_for_start]['ending_token_location'] = max_location

        return markup_by_page

    @staticmethod
    def create_row_markups_old(valid_rows, page_manager):
        valid_rows_by_page = {}
        for path in valid_rows:
            for row in valid_rows[path]:
                pg = row['page_id']
                if pg not in valid_rows_by_page:
                    valid_rows_by_page[pg] = {}
                if path not in valid_rows_by_page[pg]:
                    valid_rows_by_page[pg][path] = []
                valid_rows_by_page[pg][path].append(row)

        markup_by_page = {}
        for page in valid_rows_by_page:
            all_tokens_list = page_manager.getPage(page).tokens

            markup_by_page[page] = {}
            valid_rows_for_page = valid_rows_by_page[page]
            # print "VALID ROWS FOR: %s" % page
            # print str(valid_rows_for_page)

            earliest_latest_row_locations = {}
            for path in valid_rows_for_page:  # the path defines the row...
                earliest = -1
                latest = -1

                for row in valid_rows_for_page[path]:
                    s_loc = row['first_vis_token_loc']
                    e_loc = row['last_vis_token_loc']
                    if earliest == -1:
                        earliest = row['first_vis_token_loc']
                        latest = row['last_vis_token_loc']
                        continue

                    if s_loc < earliest:
                        earliest = s_loc
                    if e_loc > latest:
                        latest = e_loc

                earliest_latest_row_locations[path] = (earliest, latest)

            # print str(earliest_latest_row_locations)
            overlaps = []
            for pth in earliest_latest_row_locations:
                begin = earliest_latest_row_locations[pth][0]
                end = earliest_latest_row_locations[pth][1]
                if begin == -1 or end == -1:  # ill defined locations
                    continue
                if len(overlaps) == 0:  # first guy...
                    overlaps.append([pth])
                    continue

                overlap_clust = -1
                for clust_id in range(len(overlaps)):
                    cluster = overlaps[clust_id]
                    for cpath in cluster:  # could probably just find min and max of cluster and check w/ that, but easier for now...
                        p_begin = earliest_latest_row_locations[cpath][0]
                        p_end = earliest_latest_row_locations[cpath][1]

                        #  now, see if there is not  overlap...
                        if p_end < begin or p_begin > end:
                            continue
                        overlap_clust = clust_id

                if overlap_clust == -1:
                    overlaps.append([pth])
                else:
                    overlaps[overlap_clust].append(pth)

            # print "OVERLAPS"
            # print str(overlaps)
            for clust in overlaps:
                # print "===oo00 CLUSTER 00oo==="
                # print clust
                path_for_start = ""
                # left most, largest row is the beginning, so use that one as A's'
                rows_start_location = 999999999999
                rows_end_location = 0

                # first, find the member with the most rows
                max_rows = max([len(valid_rows_for_page[member]) for member in clust])

                # Ok, so the HTML between rows could have been messed up before bc we didn't know that these were
                # overlapping lists. For instance, the first row could be alone and now it's merged, so let's remake
                # the html between...

                by_location_tuples = []  # it will be (start, end, path) just to make it super easy to build the markup
                # then once we have this filled in, and we know which path demarcates each row, we simply sort
                # then iterate thru making up the rows...
                for member in clust:
                    num_rows = len(
                        valid_rows_for_page[member])  # its ok that its combined across pages... bc aggregate number
                    # print "\t--> (%d, %d): %d" % (earliest_latest_row_locations[member][0],
                    #                               earliest_latest_row_locations[member][1], num_rows)
                    # print "\t\t PATH: " + member

                    for b in valid_rows_for_page[member]:
                        by_location_tuples.append(
                            (b['first_vis_token_loc'], b['last_vis_token_loc'], member, b['page_id']))
                        # print "\t\t\t%s %s %s %s" % (str(b['first_vis_token_loc']), b['visible_text'],
                        #                              str(b['last_vis_token_loc']), b['page_id'])

                    if num_rows == max_rows:
                        if earliest_latest_row_locations[member][0] < rows_start_location:
                            rows_start_location = earliest_latest_row_locations[member][0]
                            path_for_start = member
                        if earliest_latest_row_locations[member][1] > rows_end_location:
                            # TODO: BA I think we need to extend this "if it still has overlap with the others??"
                            rows_end_location = earliest_latest_row_locations[member][1]
                print ">> Row starts at: %d and ends at %d (%s) " % (rows_start_location, rows_end_location, path_for_start)
                sorted_loc_triples = sorted(by_location_tuples)
                # print "SORTED LOCATION TRIPLES"
                # print str(by_location_tuples)

                # MariaM: 012017
                prev_start_tags = []

                # Matt M: 041317
                # now we know which path is the "start" and where each one begins and ends, so let's make the structure
                # first we want to find all entries of path_for_start
                #MariaM: 092717
                #this works with shooterswap, but will not work for jair where we have rows <div> <div odd>
                rows = [(tpl[0], tpl[3]) for tpl in sorted_loc_triples if tpl[2] == path_for_start]
                # Below works with jair, but only with jair; all tuples in sorted_loc_triples are between
                # rows_start_location and rows_end_location so below test will choose all tuples
                # rows = [(tpl[0], tpl[3]) for tpl in sorted_loc_triples
                #         if tpl[0] >= rows_start_location and tpl[1] <= rows_end_location]

                for idx in range(len(rows)):
                    lc = rows[idx][0]
                    if idx < len(rows) - 1:
                        lc_next = rows[idx + 1][0]
                        end = lc_next - 1  # we go till right before this guy
                    else:
                        end = rows_end_location
                        #MariaM: 012017
                        #this is the last row; extend it to the first start tag that did not appear
                        #in previous rows
                        prev_start_tags = list(set(prev_start_tags))
                        for token_index in range(end + 1, len(all_tokens_list)):
                            #I need just the beginning of the string
                            last_token = min(token_index + 100, len(all_tokens_list))
                            after_last_row = all_tokens_list.getTokensAsString(token_index, last_token, whitespace=True)
                            tag = re.match("<[a-z]+", after_last_row.strip())
                            if tag is not None and tag.group(0) not in prev_start_tags:
                                # we stop here with the last row;
                                end = token_index - 1
                                break
                    # TODO: BA This is where we "strip" the invisible stuff from the end
                    row_text_offset_end = 0
                    for token in reversed(all_tokens_list[lc:end+1]):
                        if token.visible:
                            break
                        row_text_offset_end += 1
                        # if token.token == '<':
                        #     break
                    end = end - row_text_offset_end
                    # TODO: BA And we add some stuff to the end of the last one because it causes some issues...
                    # if idx == len(rows)-1:
                    #     row_text_offset_last = 0
                    #     for token in all_tokens_list[end + 1:]:
                    #         if token.visible:
                    #             break
                    #         row_text_offset_last += 1
                    #         # if token.token == '<':
                    #         #     break
                    #     end = end + row_text_offset_last


                    # get the location info between...
                    # from all tokens, get all the tokens between and get the string
                    # then add the markup (include the sequency number)
                    markup_value = all_tokens_list.getTokensAsString(lc, end + 1, whitespace=True)

                    # MariaM: 012017
                    if idx < len(rows) - 1:
                        #collect all start tags
                        seen_etags = [s for s in re.findall("<[a-z]+", markup_value)]
                        prev_start_tags.extend(seen_etags)

                    markup_data = {
                        'extract': markup_value,
                        'sequence_number': idx + 1,
                        'starting_token_location': lc,
                        'ending_token_location': end
                    }
                    # print "%s: (%s)" % (page, str(markup_data))

                    if path_for_start not in markup_by_page[page]:
                        markup_by_page[page][path_for_start] = {
                            'sequence': []
                        }

                    markup_by_page[page][path_for_start]['sequence'].append(markup_data)
        for page in markup_by_page:

            for path_for_start in markup_by_page[page]:
                min_location = 9999999999
                max_location = -1
                for idx in range(len(markup_by_page[page][path_for_start]['sequence'])):
                    if markup_by_page[page][path_for_start]['sequence'][idx]['starting_token_location'] < min_location:
                        min_location = markup_by_page[page][path_for_start]['sequence'][idx]['starting_token_location']
                    if markup_by_page[page][path_for_start]['sequence'][idx]['ending_token_location'] > max_location:
                        max_location = markup_by_page[page][path_for_start]['sequence'][idx]['ending_token_location']
                markup_by_page[page][path_for_start]['starting_token_location'] = min_location
                markup_by_page[page][path_for_start]['ending_token_location'] = max_location

        return markup_by_page


    #If all pages contain the same list, remove that list; changes input markup
    @staticmethod
    def remove_duplicate_lists(markup):
        all_lists = {}
        for page in markup:
            #for each list
            for list in markup[page]:
                if list not in all_lists:
                    all_lists[list] = []
                sequence = markup[page][list]['sequence']
                #all extracted values for this list
                extract_for_page = []
                for id in range(len(sequence)):
                    extract = sequence[id]['extract']
                    extract_for_page.append(extract)
                all_lists[list].append(extract_for_page)

        #print "All Lists==================="
        #print json.dumps(all_lists, sort_keys=True, indent=2, separators=(',', ': '))

        #check if we have duplicate lists
        for list in all_lists:
            if len(all_lists[list]) == len(markup):
                first_extract = ''.join(all_lists[list][0])
                same_extract = True
                for id in range(1,len(all_lists[list])):
                    #check if first _extract is the same as all the others
                    if first_extract != ''.join(all_lists[list][id]):
                        same_extract = False
                        break
                if same_extract:
                    #remove this list
                    for page in markup:
                        markup[page].pop(list)
            else:
                for page in markup:
                    if list in markup[page]:
                        markup[page].pop(list)

        #print "New Markup==================="
        #print json.dumps(markup, sort_keys=True, indent=2, separators=(',', ': '))
        #sys.exit()

        return markup
