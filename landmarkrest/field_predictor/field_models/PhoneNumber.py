import phonenumbers
from BaseModel import BaseModel

class PhoneNumber(BaseModel):

    def __init__(self):
        super(PhoneNumber, self).__init__()

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):

        # for each value, try to parse it as a phone number
        possible_numbers = 0.0
        for value in slot_values:
            try:
                if phonenumbers.is_valid_number(phonenumbers.parse(str(value), 'US')):  # right now only for US!
                    possible_numbers += 1.0
            except:
                pass

        # confidence is the number of parses / all values
        confidence = possible_numbers / float(len(slot_values))

        return confidence
