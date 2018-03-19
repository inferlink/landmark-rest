import ConfigParser
import importlib
from pkg_resources import resource_string
import StringIO
from landmarkrest.util.util import Util


class FieldPredictor(object):

    def __init__(self):
        # TODO: use the configuration file determine if we are using InferLink's field typer, or ISI's
        self.__predictor_impl = 'InferLink'

        if self.__predictor_impl == 'InferLink':
            self.__section_name = 'Models'
            self.__field_models = self.__load_field_models()

    """
        End-point function that predicts the type of column that has been extracted (e.g., values extracted
        from multiple pages)

        @param preceding_stripes: These are the stripes preceding each of the field values.
        @param field_values: A set of values from multiple pages for the same slot
        @param following_stripes: These are the stripes coming right after the slot values
        @param confidence_threshold: Any column type that is not assigned with at least this level of confidence is
        not returned

        @note: The field_values and preceding_stripes should be ordered so they are aligned (e.g., 1st is 1st for both)

        @retun: A tuple of (field_name, confidence)
    """
    def predict(self, preceding_stripes, slot_values, following_stripes, confidence_threshold=0.0):
        if self.__predictor_impl == 'ISI':
            return {}  # TODO: this is where we call ISI's code, however they do it (service, etc.)
        else:
            return self.__inferlink_predict(preceding_stripes, slot_values, following_stripes, confidence_threshold)

    def __inferlink_predict(self, preceding_stripes, slot_values, following_stripes, confidence_threshold):
        preds = {}
        for col_type in self.__field_models:
            model = self.__field_models[col_type]
            conf = model.generate_confidence(preceding_stripes, slot_values, following_stripes)

            if conf >= confidence_threshold:
                preds[col_type] = conf

        top_x = Util.top_x_from_dict(preds, top_x=1)
        argmax = None
        if top_x:
            argmax = top_x[0]  # the first one in a one person list

        return argmax

    def __load_field_models(self):
        self.__field_models = {}

        config = ConfigParser.ConfigParser()
        config_buffer = resource_string(__name__, 'config/field_model_configs.cfg')
        buf = StringIO.StringIO(config_buffer)
        config.readfp(buf)

        for (attr, value) in config.items(self.__section_name):
            curr_class = importlib.import_module("landmarkrest.field_predictor.field_models.%s" % value)
            instance = getattr(curr_class, value)()  # reflection... booya!
            self.__field_models[attr] = instance

        return self.__field_models