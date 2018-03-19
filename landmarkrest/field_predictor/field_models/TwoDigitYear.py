from BaseModel import BaseModel


class TwoDigitYear(BaseModel):

    def __init__(self):
        super(TwoDigitYear, self).__init__()

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):
        # only care about ints for this model, so strip out anything that isn't
        valid_values = [z for z in slot_values if str(z).isdigit()]

        # two digit number
        matches = list(enumerate([(0 <= int(a) <= 99) and str(a).isdigit() and
                                  len(str(a)) == 2 for a in valid_values]))

        confidence = float(len([z for z in matches if z[1]])) / float(len(slot_values))

        return confidence
