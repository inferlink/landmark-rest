
class TreeNode(object):
    def __init__(self, value, parent, path_to_node):
        self.__value = value
        self.__parent = parent
        self.__path_to_node = path_to_node
        self.__children = {}  # key = text for child, value = count
        self.__metaData = []  # flexible list of meta-data associated with this node

    def getPath(self):
        return self.__path_to_node

    def setPath(self, path_to_node):
        self.__path_to_node = path_to_node

    def getValue(self):
        return self.__value

    def getChildren(self):
        return self.__children

    def getMetaData(self):
        return self.__metaData

    def addChild(self, child):
        if child not in self.__children:
            self.__children[child] = 0.
        self.__children[child] += 1.

    def addChildren(self, child, count):
        if child not in self.__children:
            self.__children[child] = 0
        self.__children[child] += count

    def deleteChild(self, child):
        del self.__children[child]

    def updateMetaData(self, meta):
        self.__metaData.append(meta)

    def getMetaDataForKey(self, key):
        values = []
        for struct in self.__metaData:
            values.append(struct[key])
        return values

    def addChildWithMetaData(self, child, key, value):
        self.addChild(child)
        self.updateMetaData(key, value)

    def __str__(self):
        output = "NODE: %s | CHD: %s | MT: %s | PTH: %s" % (self.__value, str(self.__children),
                                                            str(self.getMetaDataForKey('visible_text')), self.__path_to_node)
        return output

    # def __str__(self):
    #     output = ''
    #     output += "NODE: %s\n" % self.__value
    #     output += "CHILD: %s\n" % str(self.__children)
    #     output += "META: %s\n" % str(self.__metaData)
    #     output += "..........\n"
    #     return output