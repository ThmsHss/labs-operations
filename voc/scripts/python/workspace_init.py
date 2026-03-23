#!/voc/course/venv/bin/python3

from dbacademy import voc_init

# import base64
# import json
# import os
import sys

if __name__ == "__main__":

    # custom_data_file = os.getenv('VOC_CUSTOM_DATA_FILE', 'voccustomdata.txt')
    # resource_tags = json.loads(base64.b64decode(os.getenv('VOC_RESOURCE_TAGS')))

    voc_init().workspace_init()

    sys.exit(0)