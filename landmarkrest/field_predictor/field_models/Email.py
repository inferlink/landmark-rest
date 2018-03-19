from BaseModel import BaseModel
from email.utils import parseaddr


class Email(BaseModel):

    def __init__(self):
        super(Email, self).__init__()

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):

        matches = [z for z in slot_values if '@' in parseaddr(z)[1]]

        confidence = float(len([z for z in matches if z[1]])) / float(len(slot_values))

        return confidence
