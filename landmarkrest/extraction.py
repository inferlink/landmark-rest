from flask import Blueprint, request, jsonify, abort
from util.Validation import Validation

import codecs
import json
import sys

extraction_api = Blueprint('extraction_api', __name__)

@extraction_api.route('/transform', methods=['POST'])
def transform():
    if request.method == 'POST':
        post = request.get_json(force=True) 
        transforms = post.get('transforms')
        data = post.get('data')
        result = []
        for row in data:
          # default xform: force uppercase all cells
          result.append([d.upper() for d in row]) 
        return jsonify(result)
    abort(400)

@extraction_api.route('/validate', methods=['POST'])
def validate(extraction = None, validation = None):
    #data = {'extraction': extraction, 'validation': validation}
    print "Validating!"
    if request:
        if request.method == 'POST':
            data = request.get_json(force=True)

            if 'extraction' in data:
                extraction = data['extraction']
            if 'validation' in data:
                validation = data['validation']
                #validate and return extraction with metadata
                validation_obj = Validation(validation)
                validation_obj.validate_extraction(extraction)

            return jsonify(extraction=extraction)

    elif extraction and validation:
        validation_obj = Validation(validation)
        validation_obj.validate_extraction(extraction)
        return extraction

    abort(400)

def main(argv=None):

    with codecs.open("util/validation_test_1.json", "r", "utf-8") as myfile:
        file_str = myfile.read().encode('utf-8')
    validation_json = json.loads(file_str)

    with codecs.open("util/pages_extraction_test.json", "r", "utf-8") as myfile:
        page_str = myfile.read().encode('utf-8')
    page_json = json.loads(page_str)

    new_extraction = validate(page_json, validation_json)

    print new_extraction


if __name__ == "__main__":
    sys.exit(main())