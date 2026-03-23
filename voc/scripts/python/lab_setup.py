#!/voc/course/venv/bin/python3

from dbacademy import voc_init

import json
import os
from pprint import pprint
import sys

def error_exit(code, msg):
    with open(custom_data_file, 'w') as data_file:
        d = { 'error_code': str(code), 'message': msg }
        data_file.write(json.dumps(d))
        pprint(d)
    sys.exit(1)

if __name__ == "__main__":

    user = os.getenv('VOC_DB_USER_EMAIL')

    if not user:
        error_exit("INSUFFICIENT_ARGS", "VOC_DB_USER_EMAIL has not been supplied")

    voc_init().lab_setup(user)
    
    # with open(custom_data_file, 'w') as data_file:
    #     data_file.write(json.dumps(course_config))
    
    sys.exit(0)