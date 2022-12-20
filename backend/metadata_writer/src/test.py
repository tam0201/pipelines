import yaml
from workflow import *


with open("/Users/tamdo/Desktop/pipelines/backend/metadata_writer/src/sample_wf.yaml", 'r') as stream:
    try:
        parsed_yaml=yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

with open("/Users/tamdo/Desktop/pipelines/backend/metadata_writer/src/pvc.yaml", 'r') as pvc:
    try:
        pvc_yaml=yaml.safe_load(pvc)
    except yaml.YAMLError as exc:
        print(exc)

parsed_yaml = parsed_yaml.get('items')[0]
# print(parsed_yaml)
wf=transform_workflow(parsed_yaml, pvc_yaml)



with open("/Users/tamdo/Desktop/pipelines/backend/metadata_writer/src/wf_result.yaml", 'w') as template:
    try:
        yaml.dump(wf, template, default_flow_style=False)
    except yaml.YAMLError as exc:
        print(exc)