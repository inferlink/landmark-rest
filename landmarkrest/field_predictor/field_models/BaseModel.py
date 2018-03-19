import abc  # abstract base class


class BaseModel(object):

    """ Base abstract class for all rules """
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        """ Constructor for this base class """

    def generate_confidence(self, preceding_stripes, slot_values, following_stripes):
        """ This is the function that you overwrite, you just need to return a Double that
        represents the confidence. """
