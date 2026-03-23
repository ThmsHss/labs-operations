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
    ipc_data_file = os.getenv('VOC_IPC_DATA_FILE', 'vocipcdata.txt')

    if not user:
        error_exit("INSUFFICIENT_ARGS", "VOC_DB_USER_EMAIL has not been supplied")

    if not ipc_data_file:
        error_exit("INSUFFICIENT_ARGS", "VOC_IPC_DATA_FILE has not been supplied")
        
    # through INIT_BEHAVIOUR we can tell if this is a new setup or a resume, but user_setup() can figure that out on its own
    redirect_url = voc_init().user_setup(user)

    if redirect_url:
        with open(ipc_data_file, 'w') as data_file:
            data_file.write(json.dumps({'notebook_url': redirect_url}))

    sys.exit(0)


    # TODO keeping this commented code for now as a reminder that we need to propagate the resource tags to all compute resources
    # (clusters, warehouses, etc)
    # resource_tags_str = base64.b64decode(os.getenv('VOC_RESOURCE_TAGS'))
    # resource_tags = json.loads(resource_tags_str)
    
    # custom data
    # cdata_str = base64.b64decode(custom_data)
    # course_config = json.loads(cdata_str)

    # create/start cluster if specified in course config
    # if 'cluster_config' in course_config:
    #     cluster_settings = course_config['cluster_config']

    #     # merge Vocareum resource tags with anything that might have been specified in course_config
    #     if 'custom_tags' in cluster_settings:
    #         cluster_settings['custom_tags'].update(resource_tags)
    #     else:
    #         cluster_settings['custom_tags'] = resource_tags

    #     try:                
    #         db_start_or_create_cluster(w,
    #                                    user_email,
    #                                 #    settings=cluster_settings,
    #                                 #    catalog=catalog)
    #                                    settings=cluster_settings)

    #     except Exception as e:
    #         trace_it()
    #         error_exit("CLUSTER_ERROR", f"Could not start or create a cluster for {user_email}: {str(e)}")

    # # create/start SQL warehouse if specified in course config
    # if 'warehouse_config' in course_config:
    #     # merge Vocareum resource tags with anything that might have been specified in course_config
    #     if 'tags' not in course_config['warehouse_config']:
    #         course_config['warehouse_config']['tags'] = { 'custom_tags': [] }
 
    #     # tags data structure for SQL warehouses is very different than clusters
    #     course_config['warehouse_config']['tags']['custom_tags'].extend(
    #         [ {"key": k, "value": resource_tags[k]} for k in resource_tags.keys() ]
    #     )
    # try:
    #     db_start_or_create_warehouse(w,
    #                                  user_email,
    #                                  settings=course_config['warehouse_config'])

    # except Exception as e:
    #     trace_it()
    #     error_exit("SQL_ERROR", f"Could not start or create a SQL warehouse for {user_email}: {str(e)}")

    # with open(custom_data_file, 'w') as data_file:
    #     data_file.write(json.dumps(course_config))