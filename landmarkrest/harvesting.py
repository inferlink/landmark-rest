from flask import Blueprint

harvesting_api = Blueprint('harvesting_api', __name__)

@harvesting_api.route('/')
def index():
    return "Harvesting!"
