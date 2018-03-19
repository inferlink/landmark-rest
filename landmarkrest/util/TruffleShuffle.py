from common import tokenize_tuple, PAGE_BEGIN, PAGE_END
from PageManager import PageManager
from collections import OrderedDict

class TruffleShuffle(object):
    def __init__(self, tokens_only=False):
        self.__chunkBreakSeparator = '<BRK>'
        self.__number_sub_pages = 5
        self.__min_cluster_size = 5
        self.__tokens_only = tokens_only
        self.all_chunks = set()
        self.page_chunks_map = {}
        self.tokens = []
        for n in range(100000):
            self.tokens.append(())

    def add_page(self, page_id, page_html):

        if isinstance(page_html, str):
            page_unicode = unicode(page_html, 'utf-8', 'ignore')
        else:
            page_unicode = page_html
        page_normal = page_unicode

        tokens = tokenize_tuple(page_normal)
        self.__update_visible_chunks_tuple(page_id, tokens)

    def __update_visible_chunks_tuple(self, page_id, tokens):
        chunks = []
        if self.__tokens_only:
            for token in tokens:
                chunks.add(token[1])
        else:
            previous_visible = False
            invisible_token_buffer_before = []  # ""
            visible_token_buffer = []  # ""
            for token in tokens:
                token_visible = token[0]
                token_string = token[1]
                token_whitespace_text = token[2]
                if token_string == PAGE_BEGIN or token_string == PAGE_END:
                    continue
                if token_visible:
                    visible_token_buffer.append(token_string)
                    previous_visible = True
                elif previous_visible:
                    previous_visible = False
                    chunks.append(' '.join(visible_token_buffer))
                    invisible_token_buffer_before = []
                    visible_token_buffer = []

                    if token_whitespace_text and not previous_visible:
                        invisible_token_buffer_before.append(token_string)
                else:
                    if token_whitespace_text and not previous_visible:
                        invisible_token_buffer_before.append(token_string)
        page_chunks = set(chunks)
        self.all_chunks.update(page_chunks)
        self.page_chunks_map[page_id] = page_chunks

    @staticmethod
    def get_template_from_templates(template_infos=None):
        template_from_templates = PageManager()
        count = 1
        template_strings = []

        for template_info in template_infos:
            template = template_info['TEMPLATE']
            seed_page_id = template.seed_page_id
            seed_page_string = template.getPage(seed_page_id).getString()
            template_from_templates.addPage(seed_page_id, seed_page_string)

            template_string = template_info['TEMPLATE_STRING']
            template_strings.append(template_string)
            template_from_templates.addPage('template'+str(count), template_string)

        template_from_templates.learnStripes()


        return template_from_templates

    def get_chunk_separator(self):
        return self.__chunkBreakSeparator

    ##############################
    #
    # Clusters pages according to "rules". A "rule" is a list of chunks, and a "chunk" is a section of a Web page
    # that is visible to a user.
    #
    # Inputs:
    #   algorithm: 'rule_size': cluster by the size of rule from long rules to short rules
    #               'coverage' : cluster by the number of pages covered by a rule, small to big (more specific to less)
    #
    # Outputs:
    #   dict[rule] = {
    #       'MEMBERS': list of page ids (Pids from the PageManager),
    #       'ANCHOR': the anchoring chunk for this cluster
    #    }
    #   That is, each entry is a rule and its value is a dict. Note that an anchor is unique
    #   Each rule is a string of chunk_1<BRK>chunk_2<BRK>...<BRK>chunk_N
    #   it's a string to make it an index, but to use it you could break on <BRK>
    #  which you can get from the method get_chunk_separator()
    #
    ##############################
    def do_truffle_shuffle(self, algorithm='coverage'):
        self.trim_chunks()
        all_chunks, page_chunks_map = self.all_chunks, self.page_chunks_map

        chunk_counts = {}
        seen_rules = []
        rule_anchors = {}
        for chunk in all_chunks:
            pages_with_chunk = []
            for page_id in sorted(self.page_chunks_map.keys()):
                if chunk in page_chunks_map[page_id]:
                    pages_with_chunk.append(page_id)
            other_chunks = set()

            other_chunks.update(page_chunks_map[pages_with_chunk[0]])
            for page_id in pages_with_chunk:
                other_chunks.intersection_update(page_chunks_map[page_id])

            # now, find all the guys that have all of those chunks...
            if len(other_chunks) > 1:  # one token is not enough, enforce that there are at least 2...
                rule = self.__chunkBreakSeparator.join(other_chunks)
                if rule not in seen_rules:
                    chunk_counts[rule] = pages_with_chunk
                    rule_anchors[rule] = chunk

        if algorithm == 'coverage':
            counts = dict([(rule, len(chunk_counts[rule])) for rule in chunk_counts])
        else:
            # count by the size of the rule, but prefer longer,
            # so make it negative so we don't need to change sorted() call below (e.g., make rules negative
            # so that sorted small to large actually gives us longer rules (more negative) to shorter (less neg)
            counts = dict([(rule, -len(rule.split(self.__chunkBreakSeparator))) for rule in chunk_counts])

        inverted = {}
        for rl in counts:
            sz = counts[rl]
            if sz not in inverted:
                inverted[sz] = []
            inverted[sz].append(rl)
        final_clusters = {}
        already_clustered = []
        for size in sorted(inverted.keys()):
            rules = inverted[size]
            for rule in sorted(rules):
                pids = [p for p in chunk_counts[rule] if p not in already_clustered]
                if len(pids) > 1:
                    already_clustered.extend(pids)
                    final_clusters[rule] = {
                        'MEMBERS': pids,
                        'ANCHOR': rule_anchors[rule],
                        'CHUNKS': rule.strip().split(self.get_chunk_separator()),
                        'MEMBER_COUNT': len(pids)
                    }

        leftover_pids = [p for p in self.page_chunks_map.keys() if p not in already_clustered]

        trimmed_clusters = OrderedDict()
        small_clusters = []
        cluster_count = 1
        for rule in sorted(final_clusters, key=lambda x: len(final_clusters[x]['MEMBERS']), reverse=True):
            if len(final_clusters[rule]['MEMBERS']) >= self.__min_cluster_size:
                cluster_name = 'cluster' + format(cluster_count, '03')
                final_clusters[rule]['NAME'] = cluster_name
                # template = self.induce_template(final_clusters[rule]['MEMBERS'][:self.__number_sub_pages], cluster_name)

                template_info = {}
                # template_info['TEMPLATE'] = template
                # template_info['TEMPLATE_STRING'] = template.getFilledInTemplate()
                # template_info['TEMPLATE_STATS'] = template.countTokenInfoInStripesAndCleanSlots()
                final_clusters[rule]['TEMPLATE_INFO'] = template_info

                trimmed_clusters[rule] = final_clusters[rule]
            else:
                small_clusters.append(final_clusters[rule])

            cluster_count += 1

        if len(small_clusters) > 0 or len(leftover_pids) > 0:
            pids = []
            for small_cluster in small_clusters:
                pids.extend(small_cluster['MEMBERS'])
            if leftover_pids:
                pids.extend(leftover_pids)
            rule = 'other'
            cluster_name = 'cluster' + format(cluster_count, '03')

            trimmed_clusters[rule] = {
                'NAME': cluster_name,
                'MEMBERS': pids,
                'ANCHOR': rule,
                'CHUNKS': rule,
                'MEMBER_COUNT': len(pids)
            }

        # print str(len(final_clusters)) + ' clusters created.'
        # print str(len(trimmed_clusters)) + ' trimmed clusters created.'
        return trimmed_clusters

    def trim_chunks(self):
        chunks_to_remove = set()
        all_pages_sz = len(self.page_chunks_map.keys())
        for chunk in self.all_chunks:
            num_pages_with_chunk = 0
            for page_id in self.page_chunks_map.keys():
                if chunk in self.page_chunks_map[page_id]:
                    num_pages_with_chunk += 1
            if self.__tokens_only:
                if num_pages_with_chunk < 2 or num_pages_with_chunk == all_pages_sz:
                    chunks_to_remove.add(chunk)
            else:
                if num_pages_with_chunk < 10 or num_pages_with_chunk == all_pages_sz:
                    chunks_to_remove.add(chunk)

        self.all_chunks.difference_update(chunks_to_remove)
        for page_id in self.page_chunks_map.keys():
            self.page_chunks_map[page_id].difference_update(chunks_to_remove)

    # def prep_truffles_to_shuffle(self, tokens_only = False):
    #     all_chunks = set()
    #     page_chunks_map = {}
    #     for page_id in self.__page_manager.getPageIds():
    #         if tokens_only:
    #             page_chunks = set()
    #             for token in self.__page_manager.getPage(page_id).tokens:
    #                 page_chunks.add(token.token)
    #         else:
    #             page_chunks = self.__page_manager.getPageChunks(page_id)
    #         all_chunks.update(page_chunks)
    #         page_chunks_map[page_id] = page_chunks
    #
    #     chunks_to_remove = set()
    #     all_pages_sz = len(self.__page_manager.getPageIds())
    #     for chunk in all_chunks:
    #         num_pages_with_chunk = 0
    #         for page_id in self.__page_manager.getPageIds():
    #             if chunk in page_chunks_map[page_id]:
    #                 num_pages_with_chunk += 1
    #         if tokens_only:
    #             if num_pages_with_chunk < 2 or num_pages_with_chunk == all_pages_sz:
    #                 chunks_to_remove.add(chunk)
    #         else:
    #             if num_pages_with_chunk < 10 or num_pages_with_chunk == all_pages_sz:
    #                 chunks_to_remove.add(chunk)
    #
    #     all_chunks.difference_update(chunks_to_remove)
    #     for page_id in self.__page_manager.getPageIds():
    #         page_chunks_map[page_id].difference_update(chunks_to_remove)
    #
    #     return all_chunks, page_chunks_map

    def merge_clusters(self, final_clusters, train_pages_for_cluster, train_page_map, mode='template'):
        count = 1
        for rule in final_clusters:
            cluster = final_clusters[rule]
            template = PageManager()
            train_page_strings = []
            for page_id in train_pages_for_cluster[rule]:
                template.addPage(page_id, train_page_map[page_id])
                train_page_strings.append(train_page_map[page_id])

            template.learnStripes()

            template_info = dict()
            template_info['TEMPLATE'] = template
            template_info['TEMPLATE_STRING'] = template.getFilledInTemplate()
            template_info['TEMPLATE_STATS'] = template.countTokenInfoInStripesAndCleanSlots()
            cluster['TEMPLATE_INFO'] = template_info
            count += 1

        merged_clusters = {}
        for final_cluster_rule in sorted(final_clusters, key=lambda x: final_clusters[x]['NAME']):
            final_cluster_name = final_clusters[final_cluster_rule]['NAME']
            c_i = final_clusters[final_cluster_rule]
            c_i_template_info = c_i['TEMPLATE_INFO']

            did_merge = False
            for rule in merged_clusters:
                c_j = merged_clusters[rule]
                cluster_name = c_j['NAME']
                c_j_template_info = c_j['TEMPLATE_INFO']

                merged_cluster_template_info = {}
                if mode == 'clean_slots':
                    new_members = c_i['MEMBERS'][:self.__number_sub_pages]
                    new_members.extend(c_j['MEMBERS'][:self.__number_sub_pages])
                    merged_cluster_template = self.induce_template(new_members,
                                                                   cluster_name + "_mw_" + final_cluster_name)
                elif mode == 'template':  # this is template mode
                    merged_cluster_template = TruffleShuffle.get_template_from_templates(
                        template_infos=[c_i_template_info, c_j_template_info])

                merged_cluster_template_info['TEMPLATE'] = merged_cluster_template
                merged_cluster_template_info['TEMPLATE_STRING'] = merged_cluster_template.getFilledInTemplate()
                merged_cluster_template_info['TEMPLATE_STATS'] = {}
                merged_cluster_template_info['TEMPLATE_STATS'][final_cluster_name] =\
                    merged_cluster_template.countTokenInfoInStripesAndCleanSlots(
                        c_i_template_info['TEMPLATE'].seed_page_id)
                merged_cluster_template_info['TEMPLATE_STATS'][cluster_name] =\
                    merged_cluster_template.countTokenInfoInStripesAndCleanSlots(
                        c_j_template_info['TEMPLATE'].seed_page_id)

                original_clusters_info = dict()
                original_clusters_info[final_cluster_name] = c_i_template_info
                original_clusters_info[cluster_name] = c_j_template_info

                if self.__test_good_merge(original_clusters_info, merged_cluster_template_info):
                    ###add c_i to merged - really just adding the pages to it
                    merged_clusters[cluster_name]['MEMBERS'].extend(c_i['MEMBERS'])
                    merged_cluster_template_info['TEMPLATE_STATS'] = merged_cluster_template_info['TEMPLATE_STATS'][
                        final_cluster_name]
                    merged_clusters[cluster_name]['TEMPLATE_INFO'] = merged_cluster_template_info

                    ###and update the template_string to the new one
                    did_merge = True
                    break
                # print "--- cluster %s and %s test_good_merge_time: %.5f seconds ---" % \
                #       (cluster_name, final_cluster_name, test_good_merge_time)

            if not did_merge:
                merged_clusters[final_cluster_name] = c_i

        print str(len(merged_clusters)) + ' merged clusters created.'
        return merged_clusters

    def induce_template(self, cluster_members, cluster_name=None, template_string=''):
        induced_template = self.__page_manager.getSubPageManager(cluster_members, template_string)

        return induced_template

    def __test_good_merge(self, original_clusters_info, test_cluster_template_info):
        good_merges_count = 0
        original_clusters_count = 0

        for cluster_name in original_clusters_info:
            (visible_count, invisible_count, clean_slots_count_info) = original_clusters_info[cluster_name][
                'TEMPLATE_STATS']

            (new_visible_count, new_invisible_count, new_clean_slots_count_info) = \
                test_cluster_template_info['TEMPLATE_STATS'][cluster_name]

            original_clusters_count += 1

            clean_slot_count = 0
            for (clean_slot_start, clean_stop_end) in clean_slots_count_info:
                clean_slot_count += 1
                found_it = False
                for index in range(clean_slot_start, clean_stop_end + 1):
                    if found_it:
                        break
                    new_clean_slot_count = 0
                    for (new_clean_slot_start, new_clean_stop_end) in new_clean_slots_count_info:
                        new_clean_slot_count += 1
                        if index in range(new_clean_slot_start, new_clean_stop_end + 1):
                            found_it = True
                            break

            if new_visible_count <= visible_count:
                if len(new_clean_slots_count_info) >= len(clean_slots_count_info):
                    if (new_invisible_count - invisible_count) < 10:
                        good_merges_count += 1

        return good_merges_count == original_clusters_count
