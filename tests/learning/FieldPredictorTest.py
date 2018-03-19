import unittest
from landmarkrest.field_predictor.FieldPredictor import FieldPredictor
from landmarkrest.util.util import Util


class FieldPredictorTest(unittest.TestCase):
    def setUp(self):
        self.__predictor = FieldPredictor()

    def base_test(self, slot_values, correct_name):
        preceding_stripes = []
        following_stripes = []
        prediction = self.__predictor.predict(preceding_stripes, slot_values, following_stripes,
                                              confidence_threshold=0.05)
        if prediction:
            predicted_field_name = prediction[0]
            self.assertEqual(predicted_field_name, correct_name)
        else:
            self.assertEqual(None, prediction)

    def test_age_model(self):
        age = ['26', '21', '25', '22', '28', '20', '24', '23', '19', '42', '29', '27', '35', '21 - 25', '32ShavedYes',
               '30', '20ShavedYes', '33', '99', '37', '34', '38', '39', '24ShavedYes', 'Female, 22']
        pred_field = 'age'
        self.base_test(age, pred_field)

    def test_country_model(self):
        country = ['Egypt', 'United Kingdom', 'Ukraine', 'Singapore', 'Italy', 'Hong Kong', 'United States', 'Turkey',
                   'Belarus', 'Netherlands', 'United Arab Emirates', 'Statele Unite', 'India', 'Qatar', 'China',
                   'Cyprus', 'Costa Rica', 'Brazil', 'Uganda', 'Russia', 'Greece', 'International', 'Lebanon',
                   'Austria', 'Ungarn']
        pred_field = 'country'
        self.base_test(country, pred_field)

    def test_email_model(self):
        email = ['ashleyshye@yahoo.com', 'jamie.love532@gmail.com', 'amy@amytaylor.com', 'careofcadence@gmail.com',
                 'atensionmassage@yahoo', 'blackheartchicago@gmail.com', 'laurenrileyseattle@gmail.com',
                 'laryssa@lovelylaryssathenextbest', 'exoticsashaandshane@gmail.com', 'kylayoungxxx@yahoo.com',
                 'fleurdesanges12@gmail', 'meetmissrivers@gmail.com', 'laela@laelawhite.com', 'damsquarebabes@gmail',
                 'Collegebunnyt916@gmail', 'baroksme@gmail.com', 'dolcelilmama@gmail', 'exoticivanna@hotmail',
                 'seductivecassidy702@gmail', 'tawnyscontact@gmail', 'nycejobs@gmail', 'alyshatulipgfe@gmail',
                 'sitaradevi@gmail.com', 'info@xocompanions', 'twistedelegance14@gmail.com']
        pred_field = 'email'
        self.base_test(email, pred_field)

    def test_ethnicity_model(self):
        ethnicity = ['Mixed Female', 'Caucasian', 'Mixed', 'Latin', 'East Indian Female', 'Indian', 'Caucasian Female',
                     'Mediterranean', 'Ebony Female', 'Latina', 'White', 'White - preOp Provider', 'asian', 'Asian',
                     'Arab', 'Black', 'White Provider', 'Other', 'latin', 'Latina/o', 'Hispanic Female', 'caucasian',
                     'European (White)', 'Middle Eastern Female', 'Mixed White and Black African']
        pred_field = 'ethnicity'
        self.base_test(ethnicity, pred_field)

    def test_eye_color_model(self):
        eye_color = ['Brown', 'Blue', 'Grey', 'Hazel', 'Black', 'Green', 'Blue-Green', 'Grey-Green', 'Not Set',
                     'Gray', 'Grey-Blue']
        pred_field = 'eye_color'
        self.base_test(eye_color, pred_field)

    def test_four_digit_year_model(self):
        four_digit_year = ['2002', '1945', '9999', '30000', '1987']

        pred_field = 'four_digit_year'
        self.base_test(four_digit_year, pred_field)

    def test_hair_color_model(self):
        hair_color = ['Dark Brown', 'Black', 'BlackNationalityIndian', 'Brown', 'BlondeNationalityIndian',
                      'BlackNationalitySpanish', 'BrunetteNationalityLatvian', 'Blonde', 'Red',
                      'BrunetteNationalityBrazilian', 'BlondeNationalityLebanese', 'BrunetteNationalityColombian',
                      'BlackNationalityEstonian', 'BlackNationalityMongolian', 'OtherNationalityAmerican',
                      'BlackNationalityRomanian', 'BrunetteNationalitySouth Korean', 'BlackNationalityPortuguese',
                      'brunette Hair Description long', 'BlondeNationalityUkrainian', 'BlackNationalityThai',
                      'BlondeNationalityThai', 'brunette Eyes brown', 'Light Brown', 'BrunetteNationalityMoroccan']

        pred_field = 'hair_color'
        self.base_test(hair_color, pred_field)

    def test_phone_number_model(self):
        phone = ['585-755-5706', '(469) 810-4921', '989.901 1511', '202-780-1972', '1-888-888-8888']
        pred_field = 'phone_number'
        self.base_test(phone, pred_field)

    def test_url_model(self):
        urls = ['http://www.example.co.uk', 'http://example.co.uk', 'http://subdomain.example.co.uk',
                'http://www.example.com', 'http://www.example.com/', 'http://example.com', 'http://example.com/',
                'https://www.example.com', 'https://www.example.com/', 'https://example.com', 'https://example.com/',
                'www.example.com', 'www.example.com/', 'example.com?blah=blah&foo=bar', 'example.com/']
        pred_field = 'url'
        self.base_test(urls, pred_field)

    def test_city_model(self):
        cities = ['Rochester', 'Denton', 'Allentown', 'Arizona', 'Gadsden', 'Baltimore', 'Merced', 'Mississippi',
                  'Detroit', 'Calgary', 'NORFOLK', 'Portland', 'London', 'San Francisco', 'TACOMA', 'Daytona', 'TAMPA',
                  'WOODSIDE', 'Abu Dhabi Escort Agency Massage Services in Abu Dhabi Cheap', 'BROOKLYN', 'Philadelphia',
                  'OCALA', 'ANNAPOLIS', 'North Jersey', 'Vancouver']
        pred_field = 'us_city'
        self.base_test(cities, pred_field)

    def test_state_model(self):
        states = ['New York', 'Texas', 'Pennsylvania', 'Premium Phoenix', 'Phoenix', 'Alabama', 'Maryland',
                  'California',
                  'Biloxi', 'Michigan', 'VA', 'WA', 'Oklahoma', 'Ohio', 'Colorado', 'FL', 'NEW YORK', 'NY', 'MARYLAND',
                  'MD', 'TX', 'OHIO', 'OH', 'NEBRASKA', 'NE']
        pred_field = 'us_state'
        self.base_test(states, pred_field)

    def test_not_states(self):
        datas = ['This is a big block of text so let me know or I can do it myself', 'Or what about this text']
        pred_field = None
        self.base_test(datas, pred_field)

if __name__ == '__main__':
    unittest.main()
