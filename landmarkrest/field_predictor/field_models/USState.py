from ListModel import ListModel


class USState(ListModel):

    def __init__(self):
        super(USState, self).__init__()
        self.__state_list = ['alabama', 'al', 'alaska', 'ak', 'arizona', 'az', 'arkansas', 'ar', 'california', 'ca',
                             'colorado', 'co', 'connecticut', 'ct', 'delaware', 'de', 'florida', 'fl', 'georgia', 'ga',
                             'hawaii', 'hi', 'idaho', 'id', 'illinois', 'il', 'indiana', 'in', 'iowa', 'ia', 'kansas',
                             'ks', 'kentucky', 'ky', 'louisiana', 'la', 'maine', 'me', 'maryland', 'md',
                             'massachusetts', 'ma', 'michigan', 'mi', 'minnesota', 'mn', 'mississippi', 'ms',
                             'missouri', 'mo', 'montana', 'mt', 'nebraska', 'ne', 'nevada', 'nv', 'new hampshire',
                             'nh', 'new jersey', 'nj', 'new mexico', 'nm', 'new york', 'ny', 'north carolina', 'nc',
                             'north dakota', 'nd', 'ohio', 'oh', 'oklahoma', 'ok', 'oregon', 'or', 'pennsylvania',
                             'pa', 'rhode island', 'ri', 'south carolina', 'sc', 'south dakota', 'sd', 'tennessee',
                             'tn', 'texas', 'tx', 'utah', 'ut', 'vermont', 'vt', 'virginia', 'va', 'washington',
                             'wa', 'west virginia', 'wv', 'wisconsin', 'wi', 'wyoming', 'wy']

        self.__token_match_thresh = 0.5

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):
        matches = 0.0

        for value in slot_values:
            tokens = value.lower().split()
            token_matches = 0.0
            for token in tokens:
                if token in self.__state_list:
                    token_matches += 1.0

            if len(tokens) > 0 and token_matches / (float(len(tokens))) >= self.__token_match_thresh:
                matches += 1.0

        # confidence is the number of parses / all values
        confidence = matches / float(len(slot_values))

        return confidence
