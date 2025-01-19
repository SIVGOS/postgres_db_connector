import os
import json
import uuid
from dotenv import dotenv_values

def load_config(config_file):
    with open(config_file) as fin:
        data = json.load(fin)
        return data

def get_details_from_environment():
    required_fields = ['DB_NAME','DB_HOST', 'DB_USER', 'DB_PORT', 'DB_PASS', 
                        'rule_devalidation_api']
    if not set(required_fields) <= set(os.environ):
        print('The following fields must be present in the environment')
        print('\n'.join(required_fields))
        raise Exception('Required information not present in environment')
    data = {}
    for field in required_fields:
        data[field] = os.environ.get(field)
    return data


def gen_uuid():
    """Return a UUID in the string format."""
    return str(uuid.uuid1())

def gen_uuid_list(N):
    uuid_list = [str(uuid.uuid1()) for _ in range(N)]
    return uuid_list

if os.path.exists('.env'):
    ENVIRONMENTAL_VARIABLES = dotenv_values('.env')
else:
    ENVIRONMENTAL_VARIABLES = get_details_from_environment()
