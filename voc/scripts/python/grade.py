#!/voc/course/venv/bin/python3

from dbfunctions import db_import_material, db_init_workspace, db_run_grading

import base64
import json
import os
from pprint import pprint
import requests
import sys
import traceback
import uuid

def trace_it():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback, limit=50, file=sys.stdout)

def error_exit(code, msg):
    with open(vocareum_report_file, 'w') as data_file:
        d = { 'error_code': str(code), 'message': msg }
        data_file.write(json.dumps(d))
        pprint(d)
    sys.exit(1)

def dump_result(msg):
    with open(vocareum_report_file, 'w') as data_file:
        data_file.write(msg)
        pprint(msg)


if __name__ == "__main__":

    workspace_url = os.getenv('VOC_DB_WORKSPACE_URL')
    token = os.getenv('VOC_DB_API_TOKEN')
    end_lab_behavior = os.getenv('VOC_END_LAB_BEHAVIOR')
    custom_data = os.getenv('VOC_CUSTOM_DATA')
    custom_data_file = os.getenv('VOC_CUSTOM_DATA_FILE', 'voccustomdata.txt')
    user_email = os.getenv('VOC_DB_USER_EMAIL')
    custom_tags_str = os.getenv('VOC_CUSTOM_TAGS')
    
    vocareum_report_file = sys.argv[1]
    vocareum_grade_file = sys.argv[2]

    if workspace_url is None or token is None or custom_data is None or end_lab_behavior is None:
        error_exit("INSUFFICIENT_ARGS", "A required env parameter has not been supplied: workspace_url:{} / token:{} / custom_data:{} / end_lab_behavior:{}".format(workspace_url, token, custom_data, end_lab_behavior))

    # custom data
    # cdata_str = base64.b64decode(custom_data)
    # course_config = json.loads(cdata_str)

    # custom tags
    if custom_tags_str:
        custom_tags = json.loads(custom_tags_str)
        print(f"custom tags: {custom_tags}")
        orgid = custom_tags['orgid']
        partid = custom_tags['partid']
        userid = custom_tags['userid']
        print(f"orgid: {orgid}, partid: {partid}, userid: {userid}")

        course_config = db_get_course_config(partid)
    else:
        course_config = {}
        
    try:
        w = db_init_workspace(workspace_url, token)
    except Exception as e:
        trace_it()
        error_exit("WORKSPACE_INIT_ERROR", f"Could not init WorkspaceClient: {str(e)}")

    results = db_run_grading(w, course_config, user_email)

    if results:
        print("GRADING RESULTS")
        for row in results:
            print(f'{row[0]},{row[1]},{row[2] if row[2] else ""}')

        with open(vocareum_grade_file, 'w') as grade_file:
            for row in results:
                grade_file.write(f'{row[0]},{row[1]}\n')
    else:
        with open(vocareum_report_file, 'w') as grade_file:
            grade_file.write('Autograde not supported in this course\n')
    #     with open(vocareum_grade_file, 'w') as grade_file:
    #         grade_file.write('Grading not supported in this class\n')

    sys.exit(0)