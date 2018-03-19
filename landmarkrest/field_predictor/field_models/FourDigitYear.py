from BaseModel import BaseModel


class FourDigitYear(BaseModel):

    def __init__(self):
        super(FourDigitYear, self).__init__()

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):

        clean_fields = [z.decode('utf-8').encode('utf-8') for z in slot_values]

        # only care about ints for this model, so strip out anything that isn't
        valid_values = [z for z in clean_fields if str(z).isdigit()]

        # it's between 1000 and 3000, its a number and the length is 4
        matches = list(enumerate([(1000 <= int(a) <= 3000) and str(a).isdigit() and
                                  len(str(a)) == 4 for a in valid_values]))

        confidence = float(len([z for z in matches if z[1] is True])) / float(len(slot_values))

        return confidence
