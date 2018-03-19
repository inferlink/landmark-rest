from ListModel import ListModel


class HairColor(ListModel):

    def __init__(self):
        super(HairColor, self).__init__()
        self.__ethnicity_list = ['black', 'brunette', 'brunete', 'blond', 'blonde', 'curly']

        self.setList(self.__ethnicity_list)
        self.setStrict(False)
