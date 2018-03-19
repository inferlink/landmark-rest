from BaseModel import BaseModel


class Age(BaseModel):

    def __init__(self):
        super(Age, self).__init__()

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):

        clean_fields = [z.decode('utf-8').encode('utf-8') for z in slot_values]

        # only care about ints for this model, so strip out anything that isn't
        valid_values = [z for z in clean_fields if str(z).isdigit()]

        # two digit number, and assume age is 18 to 40
        matches = list(enumerate([(18 <= int(a) <= 50) and str(a).isdigit() and
                                  len(str(a)) == 2 for a in valid_values]))

        confidence = float(len([z for z in matches if z[1]])) / float(len(slot_values))

        return confidence
