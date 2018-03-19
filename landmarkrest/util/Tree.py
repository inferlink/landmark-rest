from TreeNode import TreeNode
from PageManager import PageManager
import sys
import time
from pprint import pprint
import re

class Tree:

    def __init__(self):
        self.__nodes = {}
        self.__min_occurrences = 5.0
        self.__tree = []
        self.__path_delimeter = "<BRK>"

    def getPathDelimeter(self):
        return self.__path_delimeter

    def getMinOccurrences(self):
        return self.__min_occurrences

    def setMinOccurrences(self, min_occurences):
        self.__min_occurrences = min_occurences

    def add_node_set(self, nodes, meta_data):
        for i in range(len(nodes)):
            depth = i
            parent = None
            if i > 0:
                parent = nodes[i-1]
            path_nodes = nodes[:i]
            path_nodes.reverse()
            path_to_node = self.__path_delimeter.join(path_nodes)
            self.add_node(nodes[i], depth, meta_data, path_to_node=path_to_node, parent=parent)

    def add_node(self, identifier, depth, meta_data, path_to_node=None, parent=None):
        if depth not in self.__nodes:
            self.__nodes[depth] = {}

        if identifier not in self.__nodes[depth]:
            nd = TreeNode(identifier, parent, path_to_node)
            nd.updateMetaData(meta_data)
            self.__nodes[depth][identifier] = [nd]
        else:
            # make sure we have the right one!
            found = None
            for nd_add in self.__nodes[depth][identifier]:
                if nd_add.getPath() == path_to_node:
                    found = nd_add
                    break
            if found is not None:
                found.updateMetaData(meta_data)
            else:
                nd = TreeNode(identifier, parent, path_to_node)
                nd.updateMetaData(meta_data)
                self.__nodes[depth][identifier].append(nd)

        for n in self.__nodes[depth][identifier]:
            if identifier.find("rb_loopadlisting_data") > -1:
                print "== %d %s ==" % (depth, identifier)
                # print n.getPath()
                # print str(n)

        if parent is not None:
            parent_path = self.__path_delimeter.join(path_to_node.split(self.__path_delimeter)[1:])
            for parent_node in self.__nodes[depth-1][parent]:
                if parent_node.getPath() == parent_path:
                    parent_node.addChild(identifier)

    def valid_paths(self):  # get the valid paths, along with their visible texts...
        # first, get rid of the nodes that account for very little...
        self.prune()

        # now, find all the leaves. Since each node contains it's own paths, we are fine.
        valid = {}
        for depth in self.__nodes:
            for identifier in self.__nodes[depth]:
                for node in self.__nodes[depth][identifier]:
                    if len(node.getChildren()) == 0:
                        if len(node.getMetaData()) >= self.__min_occurrences:
                            if node.getPath():
                                valid[identifier+self.__path_delimeter+node.getPath()] = node.getMetaData()
                            else:
                                valid[identifier] = node.getMetaData()
        return valid

    def cluster_meta_data(self, count=0):
        for depth in self.__nodes:
            for identifier in self.__nodes[depth]:
                for node in self.__nodes[depth][identifier]:
                    children = node.getChildren()
                    if all([len(node.getMetaData()) != children[child] for child in children]) and len(node.getMetaData()) > 2:
                        cluster = format(count, 'x')
                        for meta in node.getMetaData():
                            meta['clusters'].add(cluster)
                        count += 1
        return count

    #def get_clusters(self, cur_node=None, depth=0):
    #    if cur_node:
    #        children_metas = 0
    #        for identifier in cur_node.getChildren():
    #            for node in self.__nodes[depth][identifier]:
    #                if (depth == 1 and node.getPath() == cur_node.getValue()) or (
    #                                    depth > 1 and node.getPath() == cur_node.getValue() + self.__path_delimeter + cur_node.getPath()):
    #                    children_metas += len(node.getMetaData())
    #                    if len(node.getMetaData()) < 5:
    #                        for meta in cur_node.getMetaData():
    #                            meta['cluster'] = cur_node.getValue() + self.__path_delimeter + cur_node.getPath()
    #                        return [cur_node.getMetaData()]
    #                    break
    #        if len(cur_node.getMetaData()) > children_metas:
    #            for meta in cur_node.getMetaData():
    #                meta['cluster'] = cur_node.getValue() + self.__path_delimeter + cur_node.getPath()
    #            return [cur_node.getMetaData()]
    #        clusters = []
    #        for identifier in cur_node.getChildren():
    #            for node in self.__nodes[depth][identifier]:
    #                if (depth == 1 and node.getPath() == cur_node.getValue()) or (
    #                                depth > 1 and node.getPath() == cur_node.getValue() + self.__path_delimeter + cur_node.getPath()):
    #                    clusters.extend(self.get_clusters(node, depth+1))
    #                    break
    #        return clusters
    #    else:
    #        clusters = []
    #        if len(self.__nodes) > 0:
    #            misc_cluster = []
    #            for identifier in self.__nodes[0]:
    #                if len(self.__nodes[0][identifier][0].getMetaData()) == 1:
    #                    self.__nodes[0][identifier][0].getMetaData()[0]['cluster'] = 'miscellaneous'
    #                    misc_cluster.extend(self.__nodes[0][identifier][0].getMetaData())
    #                else:
    #                    clusters.extend(self.get_clusters(self.__nodes[0][identifier][0], 1))
    #            clusters.append(misc_cluster)
    #        return clusters

    def prune(self):
        pruners = []
        # find the possible nodes to prune: those that don't occur enough
        for depth in self.__nodes:
            for identifier in self.__nodes[depth]:
                for node in self.__nodes[depth][identifier]:
                    children = node.getChildren()
                    remove_child = []
                    for child in children:
                        if children[child] < self.__min_occurrences:
                            #print "PRUNING: %s (%d)" % ( child+self.__path_delimeter+identifier+self.__path_delimeter+node.getPath(), children[child])
                            #print str(node)
                            if node.getPath():
                                pruners.append((depth+1, child, identifier+self.__path_delimeter+node.getPath()))
                            else:
                                pruners.append((depth + 1, child, identifier))
                            remove_child.append(child)
                    for child in remove_child:
                        del children[child]

        # now we do the pruning (since we can't remove as we iterate above)
        for (depth, id, path) in pruners:
            pot_nodes = self.__nodes[depth][id]
            del_idx = 0
            for i in range(len(pot_nodes)):
                nd = pot_nodes[i]
                if nd.getPath() == path:
                    del_idx = i
                    break
            #print "REMOVE: %d %s %s : %d" % (depth, id, path, del_idx)
            del self.__nodes[depth][id][del_idx]

    # def mergeRoots(self):
    #     clusters = {}
    #     for identifier in self.__nodes[0]:
    #         attributes = self.get_attributes(identifier)
    #         key = " ".join(sorted(attributes))
    #         if key in clusters:
    #             clusters[key].append((self.__nodes[0][identifier][0], attributes))
    #         else:
    #             clusters[key] = [(self.__nodes[0][identifier][0], attributes)]
    #     for key in clusters:
    #         #print 'key ' + key
    #         merge = False
    #         unique_attributes = []
    #         if len(clusters[key]) > 1:
    #             merge = True
    #             for attribute in clusters[key][0][1]:
    #                 values = {clusters[key][i][1][attribute] for i in xrange(len(clusters[key]))}
    #                 if len(values) == len(clusters[key]):
    #                     #print 'A: ' + attribute
    #                     unique_attributes.append(attribute)
    #                 elif len(values) > 1:
    #                     #print 'B: ' + attribute
    #                     merge = False
    #                     break
    #         if merge:
    #             merged_identifier = clusters[key][0][0].getValue()
    #             #print merged_identifier
    #             for id in unique_attributes:
    #                 merged_identifier = re.sub(id+r'=["\'].*?["\']', id, merged_identifier)
    #             #print 'merge ' + merged_identifier
    #             merged_node = TreeNode(merged_identifier, None, None)
    #             for pair in clusters[key]:
    #                 datas = pair[0].getMetaData()
    #                 for data in datas:
    #                     merged_node.updateMetaData(data)
    #                 children = pair[0].getChildren()
    #                 for child in children:
    #                     merged_node.addChildren(child, children[child])
    #                     for node in self.__nodes[1][child]:
    #                         if node.getPath() == pair[0].getValue():
    #                             node.setPath(merged_identifier)
    #                 del self.__nodes[0][pair[0].getValue()]
    #             self.__nodes[0][merged_identifier] = [merged_node]
    #             self.mergePaths(merged_node, 0)
    #         else:
    #             for pair in clusters[key]:
    #                 self.mergePaths(pair[0], 0)
    #
    # def mergePaths(self, parent, depth):
    #     #merge children with identical identifiers since this should happen regardless of whether an entire cluster is merged
    #     pre_clusters = {}
    #     for identifier in parent.getChildren():
    #         if identifier not in pre_clusters:
    #             pre_clusters[identifier] = []
    #         for node in self.__nodes[depth+1][identifier]:
    #             if (depth == 0 and node.getPath() == identifier) or (
    #                     depth > 0 and node.getPath() == identifier + self.__path_delimeter + parent.getPath()):
    #                 pre_clusters[identifier].append(node)
    #     for identifier in pre_clusters:
    #         for node in pre_clusters[identifier][1:]:
    #             datas = node.getMetaData()
    #             for data in datas:
    #                 pre_clusters[identifier][0].updateMetaData(data)
    #             children = node.getChildren()
    #             for child in children:
    #                 pre_clusters[identifier][0].addChildren(child, children[child])
    #                 for child_node in self.__nodes[depth+1][child]:
    #                     if child_node.getPath() == node.getValue() + self.__path_delimeter + node.getPath():
    #                         child_node.setPath(identifier + self.__path_delimeter + pre_clusters[identifier][0].getPath())
    #             self.__nodes[depth+1][node.getValue()].remove(node)
    #
    #     clusters = {}
    #     for identifier in parent.getChildren():
    #         attributes = self.get_attributes(identifier)
    #         key = " ".join(sorted(attributes))
    #         if key not in clusters:
    #             clusters[key] = []
    #         for node in self.__nodes[depth+1][identifier]:
    #             if (depth == 0 and node.getPath() == identifier) or (
    #                     depth > 0 and node.getPath() == identifier + self.__path_delimeter + parent.getPath()):
    #                 clusters[key].append(node, attributes)
    #                 break
    #     for key in clusters:
    #         merge = False
    #         unique_attributes = []
    #         if len(clusters[key]) > 1:
    #             merge = True
    #             for attribute in clusters[key][0][1]:
    #                 values = {clusters[key][i][1][attribute] for i in xrange(len(clusters[key]))}
    #                 if len(values) == len(clusters[key]):
    #                     unique_attributes.append(attribute)
    #                 elif len(values) > 1:
    #                     merge = False
    #                     break
    #         if merge:
    #             merged_identifier = clusters[key][0][0].getValue()
    #             for id in unique_attributes:
    #                 merged_identifier = re.sub(id+r'=["\'].*?["\']', id, merged_identifier)
    #             merged_node = TreeNode(merged_identifier, None, None)
    #             for pair in clusters[key]:
    #                 datas = pair[0].getMetaData()
    #                 for data in datas:
    #                     merged_node.updateMetaData(data)
    #                 children = pair[0].getChildren()
    #                 for child in children:
    #                     merged_node.addChildren(child, children[child])
    #                     for node in self.__nodes[depth+1][child]:
    #                         if node.getPath() == pair[0].getValue() + self.__path_delimeter + pair[0].getPath():
    #                             node.setPath(merged_identifier + self.__path_delimeter + pair[0].getPath())
    #                 self.__nodes[depth+1][pair[0].getValue()].remove(pair[0])
    #             self.__nodes[depth][merged_identifier] = [merged_node]
    #             self.mergePaths(merged_node, depth+1)
    #         else:
    #             for pair in clusters[key]:
    #                 self.mergePaths(pair[0], depth+1)
    #
    # def get_attributes(self, tag):
    #     attributes = {}
    #     pairs = re.findall(r'(\s*[a-z\-_]*)=((?:".*?")|(?:\'.*?\'))', tag)
    #     for item in pairs:
    #         attributes[item[0]] = item[1]
    #     return attributes

    def display(self):
        for depth in self.__nodes:
            for identifier in self.__nodes[depth]:
                for nd in self.__nodes[depth][identifier]:
                    print depth*"\t"+str(nd)


    def learn_list_rules(self, markup_by_page, page_mgr):
        #print "MARKUP"
        #pprint(markup_by_page)
        #page_mgr.learnStripes(markups=markup_by_page)
        #this returns rules for the list rows, so we don not have to add field predictor
        #pprint(markup_by_page)
        rules = page_mgr.learnRulesFromMarkup(markup_by_page)

        # now, for each markup rule (for each list), learn a little page manager
        sublist_page_managers = {}
        for page in markup_by_page:
            for rule_name in markup_by_page[page]:
                #rule_name is the list name
                if rule_name not in sublist_page_managers:
                    sublist_page_managers[rule_name] = PageManager()
                for rid in range(len(markup_by_page[page][rule_name]['sequence'])):
                    row = markup_by_page[page][rule_name]['sequence'][rid]
                    #add to the page manager a "page" that contains the entire row extraction
                    sublist_page_managers[rule_name].addPage(page + "html%d" % rid, row['extract'])
        sublist_sub_rules = {}

        for sublist in sublist_page_managers:
            sublist_stripes = sublist_page_managers[sublist].learnStripes()
            sub_rules = sublist_page_managers[sublist].learnAllRules(sublist_stripes)
            # print '====== SUB RULES ====='
            # print sub_rules.toJson()
            for sub_rule in sub_rules.rules:
                sub_rule.removehtml = True
            sublist_sub_rules[sublist] = sub_rules  # This should match a rule name in the rules...
        count = 1

        for rule in rules.rules:
            #print "== RULE INFO =="
            #print str(rule.name)
            rule.set_sub_rules(sublist_sub_rules[rule.name])
            list_name = '_div_list' + format(count, '04')
            for page_id in markup_by_page:
                if rule.name in markup_by_page[page_id]:
                    markup_by_page[page_id][list_name] = markup_by_page[page_id].pop(rule.name)
            rule.name = list_name
            count += 1
            #print rule.toJson()
            #print "==============="
        #print rules.toJson()
        
        #print "NEW MARKUP"
        #import json
        #print json.dumps(markup_by_page, sort_keys=True, indent=2, separators=(',', ': '))
        return rules, markup_by_page

    #returns beginindex and endindex of all lists; list_markup contains only lists
    def get_list_locations(self, list_markup):
        list_locations = {}
        for page in list_markup:
            list_locations[page] = []
            #for each list
            for list in list_markup[page]:
                location = []
                beginindex = list_markup[page][list]['begin_index']
                endindex = list_markup[page][list]['end_index']
                location.append(beginindex)
                location.append(endindex)
                list_locations[page].append(location)
        return list_locations

    #returns beginindex and endindex of all lists; markup contains only the pages, no schema or urls
    def get_list_locations_cluster(self, markup):
        list_locations = {}
        for page in markup:
            list_locations[page] = []
            #for each list
            for list in markup[page]:
                if list.startswith('_div_list') or list.startswith('_list'):
                    #print "list=" + list
                    location = []
                    beginindex = markup[page][list]['begin_index']
                    endindex = markup[page][list]['end_index']
                    location.append(beginindex)
                    location.append(endindex)
                    list_locations[page].append(location)
        return list_locations


    #return True if the item with beginindex/endindex is within a list
    def item_in_list(self, page, list_locations, beginindex, endindex):
        for list_loc in list_locations[page]:
            beginlist = list_loc[0]
            endlist = list_loc[1]
            if beginlist <= beginindex and endlist >= endindex:
                return True
        return False

    # mark each item that is within a list with "in_list":"yes"
    # and each item that is NOT within a list with "in_list":"no"
    # markup contains only items (no lists)
    # return only item names NOT in a list
    def flag_items_in_list(self, markup, list_locations):
        names = []
        for page in markup:
            for item in markup[page]:
                beginindex = markup[page][item]['begin_index']
                endindex = markup[page][item]['end_index']
                if self.item_in_list(page, list_locations, beginindex, endindex):
                    markup[page][item]['in_list'] = 'yes'
                else:
                    markup[page][item]['in_list'] = 'no'
                    if item not in names:
                        names.append(item)

        return markup, names


    # mark each item that is within a list with "in_list":"yes"
    # and each item that is NOT within a list with "in_list":"no"
    # markup contains only items (no lists)
    def flag_items_in_list_return_all(self, markup, names, list_locations):
        for page in markup:
            for item in markup[page]:
                beginindex = markup[page][item]['begin_index']
                endindex = markup[page][item]['end_index']
                if self.item_in_list(page, list_locations, beginindex, endindex):
                    markup[page][item]['in_list'] = 'yes'
                else:
                    markup[page][item]['in_list'] = 'no'

        return markup, names

    # mark each item that is within a list with "in_list":"yes"
    # and each item that is NOT within a list with "in_list":"no"
    # markup contains only items (no lists)
    def flag_items_in_list_cluster(self, markup):
        schema = markup.pop("__SCHEMA__", None)
        urls = markup.pop("__URLS__", None)
        list_locations = self.get_list_locations_cluster(markup)
        for page in markup:
            for item in markup[page]:
                if item.startswith('_div_list') or item.startswith('_list'):
                    continue
                beginindex = markup[page][item]['begin_index']
                endindex = markup[page][item]['end_index']
                if self.item_in_list(page, list_locations, beginindex, endindex):
                    markup[page][item]['in_list'] = 'yes'
                else:
                    markup[page][item]['in_list'] = 'no'
        markup['__SCHEMA__'] = schema
        markup['__URLS__'] = urls

        return markup

if __name__ == '__main__':

    tree = Tree()

    import codecs
    import json

    with codecs.open('tmp.html', "r", "utf-8") as myfile:
    #with codecs.open('vol54_2.html', "r", "utf-8") as myfile:
        page_str = myfile.read().encode('utf-8')

    with codecs.open('tmp2.html', "r", "utf-8") as myfile:
    #with codecs.open('vol49_2.html', "r", "utf-8") as myfile:
        page_str2 = myfile.read().encode('utf-8')

    from TreeListLearner import TreeListLearner
    (paths, pg_mgr) = TreeListLearner.learn_lists({'tmp1.html': page_str, 'tmp2.html': page_str2}, filter_tags_by='div')
    #(paths, pg_mgr) = TreeListLearner.learn_lists({'tmp1.html': page_str, 'tmp2.html': page_str2},filter_tags_by='th')

    #for tok in pg_mgr._pages['tmp1.html'].tokens:
    #    print tok.token + " " + str(tok.token_location)
    #sys.exit()

    print "=== PATHS ==="
    print json.dumps(paths, sort_keys=True, indent=2, separators=(',', ': '))

    #sys.exit()

    markup_by_page = TreeListLearner.create_row_markups(paths, pg_mgr)

    #sys.exit()
    print "=== MARKUP ==="
    print json.dumps(markup_by_page, sort_keys=True, indent=2, separators=(',', ': '))
    #sys.exit()
    print "== learn sub rules =="
    rules, markup_by_page_rename = tree.learn_list_rules(markup_by_page, pg_mgr)

    print "=== ALL RULES ==="
    print rules.toJson()
    #sys.exit()

    print "== testing rules page 1 =="
    extraction_list = rules.extract(page_str)

    # from extraction.Landmark import flattenResult
    print json.dumps(extraction_list, sort_keys=True, indent=2, separators=(',', ': '))

    print "== testing rules page 2 =="
    extraction_list = rules.extract(page_str2)

    # from extraction.Landmark import flattenResult
    print json.dumps(extraction_list, sort_keys=True, indent=2, separators=(',', ': '))
