from BaseModel import BaseModel


class ListModel(BaseModel):

    def __init__(self):
        super(ListModel, self).__init__()
        self.__match_list = []
        self.__strict_match = True  # whether you do partial token match or full match

    def setList(self, match_list):
        self.__match_list = match_list

    def setStrict(self, strict):
        self.__strict_match = strict

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):

        matches = 0.0

        for value in slot_values:
            if self.__strict_match:
                if value.lower() in self.__match_list:
                    matches += 1.0
            else:
                # now this is token based...
                tokens = value.lower().split()
                for token in tokens:
                    if token in self.__match_list:
                        matches += 1.0
                        break

        # confidence is the number of parses / all values
        confidence = matches / float(len(slot_values))

        return confidence
