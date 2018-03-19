from ListModel import ListModel


class Ethnicity(ListModel):

    def __init__(self):
        super(Ethnicity, self).__init__()
        self.__ethnicity_list = ['black', 'asian', 'latin', 'latina', 'latino', 'white']

        self.setList(self.__ethnicity_list)
        self.setStrict(False)
