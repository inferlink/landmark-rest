from ListModel import ListModel


class EyeColor(ListModel):

    def __init__(self):
        super(EyeColor, self).__init__()
        self.__ethnicity_list = ['black', 'blue', 'green', 'hazel', 'grey', 'gray']

        self.setList(self.__ethnicity_list)
        self.setStrict(False)
