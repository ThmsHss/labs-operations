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
    end_lab_behavior = os.getenv('VOC_END_LAB_BEHAVIOR')

    if not user:
        error_exit("INSUFFICIENT_ARGS", "VOC_DB_USER_EMAIL has not been supplied")

    if not end_lab_behavior:
        error_exit("INSUFFICIENT_ARGS", "VOC_END_LAB_BEHAVIOR has not been supplied")

    # custom_data = os.getenv('VOC_CUSTOM_DATA')
    # custom_data_file = os.getenv('VOC_CUSTOM_DATA_FILE', 'voccustomdata.txt')
    # user_email = os.getenv('VOC_DB_USER_EMAIL')

    # custom data
    # cdata_str = base64.b64decode(custom_data)
    # cdata = json.loads(cdata_str)
    
    # print("CDATA = {}".format(cdata))

    match end_lab_behavior:
        case 'stop':
            voc_init().lab_end_stop(user)
        
        case 'terminate':
            voc_init().lab_end_terminate(user)

        case _:
            error_exit("INCORRECT_ARGS", "end_lab_behavior '{}' must be one of 'stop' or 'terminate'".format(end_lab_behavior))

    # with open(custom_data_file, 'w') as data_file:
    #     d = {}
    #     data_file.write(json.dumps(d))
        
    sys.exit(0)