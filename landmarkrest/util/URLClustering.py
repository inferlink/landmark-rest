import re
import logging

class URLTree:

    def __init__(self, pages, domain):
        self.root = URLNode(segment=domain)
        self.root.descendantLeaves = len(pages)
        allNodes = []
        for page in pages:
            #editedUrl = re.sub("/?#[^/]*?(/$|$)", "", page[1].lower())
            editedUrl = re.sub(r"\?.+?=[^/]+?\\?$","", page[1].lower())
            segments = filter(None, re.split("/([^/\?]+(?=\?))|/", editedUrl))[1:]
            if not segments:
                continue
            segments[0] = re.sub(r'\Awww\.', '', segments[0])
            lastNode = self.root
            for index, segment in enumerate(segments):
                if len(allNodes) <= index:
                    allNodes.append([])
                currentNode = URLNode(segment=segment)
                lastNode.children.append(currentNode)
                currentNode.parent = lastNode
                allNodes[index].append(currentNode)
                lastNode = currentNode
            lastNode.page = page

        for i in xrange(0, len(allNodes)):
            mergedNodes = []
            for node in allNodes[i]:
                merged = False
                for mergedNode in mergedNodes:
                    if mergedNode.segment == node.segment:
                        mergedNode.zip(node)
                        merged = True
                        break
                if not merged:
                    mergedNodes.append(node)
        self.finalMerge()


    def finalMerge(self):
        self.finalMergeHelper(self.root)

    def finalMergeHelper(self, node):
        mergedNodes = []
        toRemove = []
        for child in node.children:
            merged = False
            for mergedNode in mergedNodes:
                if mergedNode.partialMatch(child):
                    mergedNode.children.extend(child.children)
                    mergedNode.descendantLeaves += child.descendantLeaves
                    for grandchild in child.children:
                        grandchild.parent = mergedNode
                    child.children = []
                    if child.page is None or mergedNode.page is None:
                        if child.page is not None:
                            mergedNode.page = child.page
                        toRemove.append(child)
                    merged = True
                    break
            if not merged:
                mergedNodes.append(child)
        for child in toRemove:
            node.children.remove(child)

        toRemove = []
        longBranches = filter(lambda x: len(x.children) > 0, node.children)
        leaves = len(node.children) - len(longBranches)
        twigs = self.filterTwigs(node.descendantLeaves - leaves, longBranches)
        if len(twigs) > 1:
            mergeInto = twigs[0]
            for child in twigs[1:]:
                mergeInto.children.extend(child.children)
                mergeInto.descendantLeaves += child.descendantLeaves
                for grandchild in child.children:
                    grandchild.parent = mergeInto
                child.children = []
                if child.page is None or mergeInto.page is None:
                    if child.page is not None:
                        mergeInto.page = child.page
                    toRemove.append(child)
        for child in toRemove:
            node.children.remove(child)
        for child in node.children:
            self.finalMergeHelper(child)

    def filterTwigs(self, totalLeaves, branches):
        remove = None
        for branch in branches:
            if branch.descendantLeaves**2 > totalLeaves:
                remove = branch
                break
        if remove is None:
            return branches
        else:
            branches.remove(remove)
            return self.filterTwigs(totalLeaves - branch.descendantLeaves, branches)

    def getClusters(self):
        return self.getClustersHelper(self.root)

    def getClustersHelper(self, node):
        immediateLeaves = 0
        currentClusters = dict()
        deepClusters = []

        for child in node.children:
            if child.page is not None:
                immediateLeaves += 1
                pattern = re.sub("[0-9]+","[0-9]+", re.escape(child.segment)) + "$"
                if pattern not in currentClusters.keys():
                    currentClusters[pattern] = []
                currentClusters[pattern].append(child.page)
            deepClusters.extend(self.getClustersHelper(child))

        remove = 0
        while remove is not None:
            remove = None
            for key in currentClusters:
                if len(currentClusters[key])**2 > immediateLeaves:
                    deepClusters.append((currentClusters[key], re.escape(node.segment) + "/" + key))
                    remove = key
                    immediateLeaves -= len(currentClusters[key])
                    break
            if remove is not None:
                del currentClusters[remove]

        if len(currentClusters.keys()) > 0:
            newCluster = ([], re.escape(node.segment) + "/[^/]+$")
            for key in currentClusters:
                newCluster[0].extend(currentClusters[key])
            deepClusters.append(newCluster)
        return deepClusters

class URLNode:

    def __init__(self, segment=None):
        self.page = None
        self.segment = segment
        self.children = []
        self.parent = None
        self.descendantLeaves = 1

    def zip(self, node):
        if self.page is not None and node.page is not None:
            return
        for child in node.children:
            child.parent = self
        self.children.extend(node.children)
        if node.page is not None:
            node.parent.changeDescendants(-node.descendantLeaves-1)
            self.descendantLeaves += node.descendantLeaves
            self.parent.changeDescendants(node.descendantLeaves + 1)
            self.page = node.page
        else:
            node.parent.changeDescendants(-node.descendantLeaves)
            self.changeDescendants(node.descendantLeaves)
        if node.parent is not None:
            node.parent.children.remove(node)
            if self.parent is not None and self.parent != node.parent:
                self.parent.zip(node.parent)

    def partialMatch(self, node):
        pattern = re.escape(self.segment)
        matches = []
        for match in re.finditer(r"(?:\?.+?=)(.+?)(?=\\\?|$)", pattern):
            matches.append(match)
        for match in reversed(matches):
            pattern = '%s%s%s' % (pattern[0:match.start(1)], "[^\?]+", pattern[match.end(1):])
        pattern = re.sub("[0-9]+", "[0-9]+", pattern) + '$'
        return re.match(pattern, node.segment) is not None

    def changeDescendants(self, x):
        self.descendantLeaves += x
        if self.parent is not None:
            self.parent.changeDescendants(x)


def cluster_urls(pages):
    splitByDomain = dict()
    urls = set()
    for page in pages:
        url = re.sub("/?#[^/]*?(/$|$)", "", page[1])
        if url in urls:
            continue
        else:
            urls.add(url)
            if url != page[1]:
                page = (page[0], url)
        domain_match = re.match(r"https?://(?:www\.)?(.+?)(/|$)", page[1])
        if domain_match:
            domain = domain_match.group(1)
            if domain not in splitByDomain:
                splitByDomain[domain] = []
            splitByDomain[domain].append(page)
        else:
            logging.info('error clustering ' + str(page[1]))

    clusters = []
    for domain in splitByDomain:
        tree = URLTree(splitByDomain[domain], domain)
        clusters.extend(tree.getClusters())

    smallClusters = []
    for cluster in clusters:
        if len(cluster[0]) < 5:
            smallClusters.append(cluster)

    miscCluster = ([], "other")
    for cluster in smallClusters:
        miscCluster[0].extend(cluster[0])
        clusters.remove(cluster)
    clusters.append(miscCluster)

    return clusters