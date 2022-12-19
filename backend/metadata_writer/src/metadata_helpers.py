# Copyright 2019 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os, re
import sys
import string
import random
import copy
import warnings

from typing import (Any, Callable, Dict, List, Optional, Sequence, Tuple,
                    TypeVar, Union)
import kubernetes
import ml_metadata
from time import sleep
from ml_metadata.proto import metadata_store_pb2
from ml_metadata.metadata_store import metadata_store
from ipaddress import ip_address, IPv4Address 
import k8sutils
import podutils
# from kfp.dsl import PipelineVolume
from kubernetes.client.models import (V1ObjectMeta, V1ResourceRequirements,
                                      V1PersistentVolumeClaimSpec, V1Volume,
                                      V1PersistentVolumeClaim, V1VolumeMount,
                                      V1TypedLocalObjectReference)


def value_to_mlmd_value(value) -> metadata_store_pb2.Value:
    if value is None:
        return metadata_store_pb2.Value()
    if isinstance(value, int):
        return metadata_store_pb2.Value(int_value=value)
    if isinstance(value, float):
        return metadata_store_pb2.Value(double_value=value)
    return metadata_store_pb2.Value(string_value=str(value))


def connect_to_mlmd() -> metadata_store.MetadataStore:
    metadata_service_host = os.environ.get(
        'METADATA_GRPC_SERVICE_SERVICE_HOST', 'metadata-grpc-service')
    metadata_service_port = int(os.environ.get(
        'METADATA_GRPC_SERVICE_SERVICE_PORT', 8080))

    mlmd_connection_config = metadata_store_pb2.MetadataStoreClientConfig(
        host="[{}]".format(metadata_service_host) if isIPv6(metadata_service_host) else metadata_service_host,
        port=metadata_service_port,
    )

    # Checking the connection to the Metadata store.
    for _ in range(100):
        try:
            mlmd_store = metadata_store.MetadataStore(mlmd_connection_config)
            # All get requests fail when the DB is empty, so we have to use a put request.
            # TODO: Replace with _ = mlmd_store.get_context_types() when https://github.com/google/ml-metadata/issues/28 is fixed
            _ = mlmd_store.put_execution_type(
                metadata_store_pb2.ExecutionType(
                    name="DummyExecutionType",
                )
            )
            return mlmd_store
        except Exception as e:
            print('Failed to access the Metadata store. Exception: "{}"'.format(str(e)), file=sys.stderr)
            sys.stderr.flush()
            sleep(1)

    raise RuntimeError('Could not connect to the Metadata store.')


def get_or_create_artifact_type(store, type_name, properties: dict = None) -> metadata_store_pb2.ArtifactType:
    try:
        artifact_type = store.get_artifact_type(type_name=type_name)
        return artifact_type
    except:
        artifact_type = metadata_store_pb2.ArtifactType(
            name=type_name,
            properties=properties,
        )
        artifact_type.id = store.put_artifact_type(artifact_type) # Returns ID
        return artifact_type


def get_or_create_execution_type(store, type_name, properties: dict = None) -> metadata_store_pb2.ExecutionType:
    try:
        execution_type = store.get_execution_type(type_name=type_name)
        return execution_type
    except:
        execution_type = metadata_store_pb2.ExecutionType(
            name=type_name,
            properties=properties,
        )
        execution_type.id = store.put_execution_type(execution_type) # Returns ID
        return execution_type


def get_or_create_context_type(store, type_name, properties: dict = None) -> metadata_store_pb2.ContextType:
    try:
        context_type = store.get_context_type(type_name=type_name)
        return context_type
    except:
        context_type = metadata_store_pb2.ContextType(
            name=type_name,
            properties=properties,
        )
        context_type.id = store.put_context_type(context_type) # Returns ID
        return context_type


def create_artifact_with_type(
    store,
    uri: str,
    type_name: str,
    properties: dict = None,
    type_properties: dict = None,
    custom_properties: dict = None,
) -> metadata_store_pb2.Artifact:
    artifact_type = get_or_create_artifact_type(
        store=store,
        type_name=type_name,
        properties=type_properties,
    )
    artifact = metadata_store_pb2.Artifact(
        uri=uri,
        type_id=artifact_type.id,
        properties=properties,
        custom_properties=custom_properties,
    )
    artifact.id = store.put_artifacts([artifact])[0]
    return artifact


def create_execution_with_type(
    store,
    type_name: str,
    properties: dict = None,
    type_properties: dict = None,
    custom_properties: dict = None,
) -> metadata_store_pb2.Execution:
    execution_type = get_or_create_execution_type(
        store=store,
        type_name=type_name,
        properties=type_properties,
    )
    execution = metadata_store_pb2.Execution(
        type_id=execution_type.id,
        properties=properties,
        custom_properties=custom_properties,
    )
    execution.id = store.put_executions([execution])[0]
    return execution


def create_context_with_type(
    store,
    context_name: str,
    type_name: str,
    properties: dict = None,
    type_properties: dict = None,
    custom_properties: dict = None,
) -> metadata_store_pb2.Context:
    # ! Context_name must be unique
    context_type = get_or_create_context_type(
        store=store,
        type_name=type_name,
        properties=type_properties,
    )
    context = metadata_store_pb2.Context(
        name=context_name,
        type_id=context_type.id,
        properties=properties,
        custom_properties=custom_properties,
    )
    context.id = store.put_contexts([context])[0]
    return context


import functools
@functools.lru_cache(maxsize=128)
def get_context_by_name(
    store,
    context_name: str,
) -> metadata_store_pb2.Context:
    matching_contexts = [context for context in store.get_contexts() if context.name == context_name]
    assert len(matching_contexts) <= 1
    if len(matching_contexts) == 0:
        raise ValueError('Context with name "{}" was not found'.format(context_name))
    return matching_contexts[0]


def get_or_create_context_with_type(
    store,
    context_name: str,
    type_name: str,
    properties: dict = None,
    type_properties: dict = None,
    custom_properties: dict = None,
) -> metadata_store_pb2.Context:
    try:
        context = get_context_by_name(store, context_name)
    except:
        context = create_context_with_type(
            store=store,
            context_name=context_name,
            type_name=type_name,
            properties=properties,
            type_properties=type_properties,
            custom_properties=custom_properties,
        )
        return context

    # Verifying that the context has the expected type name
    context_types = store.get_context_types_by_id([context.type_id])
    assert len(context_types) == 1
    if context_types[0].name != type_name:
        raise RuntimeError('Context "{}" was found, but it has type "{}" instead of "{}"'.format(context_name, context_types[0].name, type_name))
    return context


def create_new_execution_in_existing_context(
    store,
    execution_type_name: str,
    context_id: int,
    properties: dict = None,
    execution_type_properties: dict = None,
    custom_properties: dict = None,
) -> metadata_store_pb2.Execution:
    execution = create_execution_with_type(
        store=store,
        properties=properties,
        custom_properties=custom_properties,
        type_name=execution_type_name,
        type_properties=execution_type_properties,
    )
    association = metadata_store_pb2.Association(
        execution_id=execution.id,
        context_id=context_id,
    )

    store.put_attributions_and_associations([], [association])
    return execution


RUN_CONTEXT_TYPE_NAME = "KfpRun"
KFP_EXECUTION_TYPE_NAME_PREFIX = 'components.'

ARTIFACT_IO_NAME_PROPERTY_NAME = "name"
EXECUTION_COMPONENT_ID_PROPERTY_NAME = "component_id"# ~= Task ID

#TODO: Get rid of these when https://github.com/tensorflow/tfx/issues/905 and https://github.com/kubeflow/pipelines/issues/2562 are fixed
ARTIFACT_PIPELINE_NAME_PROPERTY_NAME = "pipeline_name"
EXECUTION_PIPELINE_NAME_PROPERTY_NAME = "pipeline_name"
CONTEXT_PIPELINE_NAME_PROPERTY_NAME = "pipeline_name"
ARTIFACT_RUN_ID_PROPERTY_NAME = "run_id"
EXECUTION_RUN_ID_PROPERTY_NAME = "run_id"
CONTEXT_RUN_ID_PROPERTY_NAME = "run_id"

KFP_POD_NAME_EXECUTION_PROPERTY_NAME = 'kfp_pod_name'

ARTIFACT_ARGO_ARTIFACT_PROPERTY_NAME = 'argo_artifact'


def get_or_create_run_context(
    store,
    run_id: str,
) -> metadata_store_pb2.Context:
    context = get_or_create_context_with_type(
        store=store,
        context_name=run_id,
        type_name=RUN_CONTEXT_TYPE_NAME,
        type_properties={
            CONTEXT_PIPELINE_NAME_PROPERTY_NAME: metadata_store_pb2.STRING,
            CONTEXT_RUN_ID_PROPERTY_NAME: metadata_store_pb2.STRING,
        },
        properties={
            CONTEXT_PIPELINE_NAME_PROPERTY_NAME: metadata_store_pb2.Value(string_value=run_id),
            CONTEXT_RUN_ID_PROPERTY_NAME: metadata_store_pb2.Value(string_value=run_id),
        },
    )
    return context


def create_new_execution_in_existing_run_context(
    store,
    execution_type_name: str,
    context_id: int,
    pod_name: str,
    # TODO: Remove when UX stops relying on thsese properties
    pipeline_name: str = None,
    run_id: str = None,
    instance_id: str = None,
    custom_properties = None,
) -> metadata_store_pb2.Execution:
    pipeline_name = pipeline_name or 'Context_' + str(context_id) + '_pipeline'
    run_id = run_id or 'Context_' + str(context_id) + '_run'
    instance_id = instance_id or execution_type_name
    mlmd_custom_properties = {}
    for property_name, property_value in (custom_properties or {}).items():
        mlmd_custom_properties[property_name] = value_to_mlmd_value(property_value)
    mlmd_custom_properties[KFP_POD_NAME_EXECUTION_PROPERTY_NAME] = metadata_store_pb2.Value(string_value=pod_name)
    return create_new_execution_in_existing_context(
        store=store,
        execution_type_name=execution_type_name,
        context_id=context_id,
        execution_type_properties={
            EXECUTION_PIPELINE_NAME_PROPERTY_NAME: metadata_store_pb2.STRING,
            EXECUTION_RUN_ID_PROPERTY_NAME: metadata_store_pb2.STRING,
            EXECUTION_COMPONENT_ID_PROPERTY_NAME: metadata_store_pb2.STRING,
        },
        # TODO: Remove when UX stops relying on thsese properties
        properties={
            EXECUTION_PIPELINE_NAME_PROPERTY_NAME: metadata_store_pb2.Value(string_value=pipeline_name), # Mistakenly used for grouping in the UX
            EXECUTION_RUN_ID_PROPERTY_NAME: metadata_store_pb2.Value(string_value=run_id),
            EXECUTION_COMPONENT_ID_PROPERTY_NAME: metadata_store_pb2.Value(string_value=instance_id), # should set to task ID, not component ID
        },
        custom_properties=mlmd_custom_properties,
    )


def create_new_artifact_event_and_attribution(
    store,
    execution_id: int,
    context_id: int,
    uri: str,
    type_name: str,
    event_type: metadata_store_pb2.Event.Type,
    properties: dict = None,
    artifact_type_properties: dict = None,
    custom_properties: dict = None,
    artifact_name_path: metadata_store_pb2.Event.Path = None,
    milliseconds_since_epoch: int = None,
) -> metadata_store_pb2.Artifact:
    artifact = create_artifact_with_type(
        store=store,
        uri=uri,
        type_name=type_name,
        type_properties=artifact_type_properties,
        properties=properties,
        custom_properties=custom_properties,
    )
    event = metadata_store_pb2.Event(
        execution_id=execution_id,
        artifact_id=artifact.id,
        type=event_type,
        path=artifact_name_path,
        milliseconds_since_epoch=milliseconds_since_epoch,
    )
    store.put_events([event])

    attribution = metadata_store_pb2.Attribution(
        context_id=context_id,
        artifact_id=artifact.id,
    )
    store.put_attributions_and_associations([attribution], [])

    return artifact


def link_execution_to_input_artifact(
    store,
    execution_id: int,
    uri: str,
    input_name: str,
) -> metadata_store_pb2.Artifact:
    artifacts = store.get_artifacts_by_uri(uri)
    if len(artifacts) == 0:
        print('Error: Not found upstream artifact with URI={}.'.format(uri), file=sys.stderr)
        return None
    if len(artifacts) > 1:
        print('Error: Found multiple artifacts with the same URI. {} Using the last one..'.format(artifacts), file=sys.stderr)

    artifact = artifacts[-1]

    event = metadata_store_pb2.Event(
        execution_id=execution_id,
        artifact_id=artifact.id,
        type=metadata_store_pb2.Event.INPUT,
        path=metadata_store_pb2.Event.Path(
            steps=[
                metadata_store_pb2.Event.Path.Step(
                    key=input_name,
                ),
            ]
        ),
    )
    store.put_events([event])
    return artifact


def create_new_output_artifact(
    store,
    execution_id: int,
    context_id: int,
    uri: str,
    type_name: str,
    output_name: str,
    run_id: str = None,
    argo_artifact: dict = None,
) -> metadata_store_pb2.Artifact:
    custom_properties = {
        ARTIFACT_IO_NAME_PROPERTY_NAME: metadata_store_pb2.Value(string_value=output_name),
    }
    if run_id:
        custom_properties[ARTIFACT_PIPELINE_NAME_PROPERTY_NAME] = metadata_store_pb2.Value(string_value=str(run_id))
        custom_properties[ARTIFACT_RUN_ID_PROPERTY_NAME] = metadata_store_pb2.Value(string_value=str(run_id))
    if argo_artifact:
        custom_properties[ARTIFACT_ARGO_ARTIFACT_PROPERTY_NAME] = metadata_store_pb2.Value(string_value=json.dumps(argo_artifact, sort_keys=True))
    return create_new_artifact_event_and_attribution(
        store=store,
        execution_id=execution_id,
        context_id=context_id,
        uri=uri,
        type_name=type_name,
        event_type=metadata_store_pb2.Event.OUTPUT,
        artifact_name_path=metadata_store_pb2.Event.Path(
            steps=[
                metadata_store_pb2.Event.Path.Step(
                    key=output_name,
                    #index=0,
                ),
            ]
        ),
        custom_properties=custom_properties,
        #milliseconds_since_epoch=int(datetime.now(timezone.utc).timestamp() * 1000), # Happens automatically
    )

def isIPv6(ip: str) -> bool: 
    try: 
        return False if type(ip_address(ip)) is IPv4Address else True
    except Exception as e: 
        print('Error: Exception:{}'.format(str(e)), file=sys.stderr)
        sys.stderr.flush()

##################
##################
#### Snapshot ####
##################
##################

VOLUME_MODE_RWO = ["ReadWriteOnce"]
VOLUME_MODE_RWM = ["ReadWriteMany"]
VOLUME_MODE_ROM = ["ReadOnlyMany"]


def snapshot_pvc(snapshot_name, pvc_name, labels, annotations):
    """Perform a snapshot over a PVC."""
    if annotations == {}:
        annotations = {"access_mode": get_pvc_access_mode(pvc_name)}
    snapshot_resource = {
        "apiVersion": "snapshot.storage.k8s.io/v1beta1",
        "kind": "VolumeSnapshot",
        "metadata": {
            "name": snapshot_name,
            "annotations": annotations,
            "labels": labels
        },
        "spec": {
            "volumeSnapshotClassName": get_snapshotclass_name(pvc_name),
            "source": {"persistentVolumeClaimName": pvc_name}
        }
    }
    co_client = k8sutils.get_co_client()
    namespace = podutils.get_namespace()
    print("Taking a snapshot of PVC %s in namespace %s ...",
             (pvc_name, namespace))
    task_info = co_client.create_namespaced_custom_object(
        group="snapshot.storage.k8s.io",
        version="v1",
        namespace=namespace,
        plural="volumesnapshots",
        body=snapshot_resource)

    return task_info

def snapshot_step():
    """Take snapshots of the current Notebook's PVCs and store its metadata."""
    volumes = [(path, volume.name, size)
               for path, volume, size in podutils.list_volumes()]
    namespace = podutils.get_namespace()
    pod_name = podutils.get_pod_name()
    resources = get_step_resources()
    print("Taking a snapshot of notebook %s in namespace %s ...",
             (pod_name, namespace))
    version_uuid = generate_uuid()
    snapshot_names = []
    for vol in volumes:
        annotations = {}
        if resources:
            for key in resources:
                annotations[key] = resources[key]
        annotations["access_mode"] = get_pvc_access_mode(vol[1])
        annotations["container_image"] = podutils.get_docker_base_image()
        annotations["volume_path"] = vol[0]
        snapshot_name = "nb-snapshot-" + version_uuid + "-" + vol[1]
        snapshot_pvc(
            snapshot_name=snapshot_name,
            pvc_name=vol[1],
            annotations=annotations,
            labels={"container_name": podutils.get_container_name(),
                    "version_uuid": version_uuid,
                    "is_workspace_dir": str(podutils.is_workspace_dir(vol[0]))}
        )
        snapshot_names.append(snapshot_name)
    return snapshot_names


def get_step_resources():
    """Get the resource limits and requests of the current Notebook."""
    nb_name = podutils.get_container_name()
    namespace = podutils.get_namespace()
    co_client = k8sutils.get_co_client()
    get_resource = co_client.get_namespaced_custom_object(
        name=nb_name,
        group="argoproj.io",
        version="v1alpha1",
        namespace=namespace,
        plural="Workflow")["spec"]["template"]["container"][0]
    resource_spec = get_resource["resources"]
    resource_conf = {}
    for resource_type in resource_spec.items():
        for key in resource_type[1]:
            resource_conf[resource_type[0] + "_" + key] = resource_type[1][key]
    return resource_conf


def get_snapshotclass_name(pvc_name, label_selector=""):
    """Get the Volume Snapshot Class Name for a PVC."""
    client = k8sutils.get_v1_client()
    namespace = podutils.get_namespace()
    pvc = client.read_namespaced_persistent_volume_claim(pvc_name, namespace)
    ann = pvc.metadata.annotations
    provisioner = ann.get("volume.beta.kubernetes.io/storage-provisioner",
                          None)
    snapshotclasses = podutils.get_snapshotclasses(label_selector)
    return [snapclass_name["metadata"]["name"] for snapclass_name in
            snapshotclasses if snapclass_name["driver"] == provisioner][0]

def get_pvc_access_mode(pvc_name):
    """Get the access mode of a PVC."""
    client = k8sutils.get_v1_client()
    namespace = podutils.get_namespace()
    pvc = client.read_namespaced_persistent_volume_claim(pvc_name, namespace)
    return pvc.spec.access_modes[0]

def generate_uuid():
    """Generate a 8 character UUID for snapshot names and versioning."""
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choices(alphabet, k=8))

def create_pvc(wf_name):
    client = k8sutils.get_v1_client()
    namespace = podutils.get_namespace()

    pvc_metadata = V1ObjectMeta(
        name=wf_name + "pvc" + generate_uuid()
    )
    requested_resources = V1ResourceRequirements(requests={"storage": "1Gi"})
    pvc_spec = V1PersistentVolumeClaimSpec(
        access_modes=VOLUME_MODE_RWM,
        resources=requested_resources,
        storage_class_name="longhorn")
    k8s_resource = V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=pvc_metadata,
        spec=pvc_spec)
    ns_pvc = client.create_namespaced_persistent_volume_claim(namespace, k8s_resource)
    return ns_pvc

def mount_pvc_to_pipeline(pvolumes: Dict[str, V1Volume]):
    client=k8sutils.get_v1_client()
    namespace = podutils.get_namespace()
    for mountpath, pvolume in pvolumes.items():
        volume_mount = V1VolumeMount(name=pvolume.name, mount_path=mountpath)
    return client.patch_namespaced_persistent_volume_claim(pvolume.name, namespace=namespace, body={"status":{"mounts":[volume_mount]}})

def patch_pvc_storage(pvc_name, size):
    client=k8sutils.get_v1_client
    namespace=podutils.get_namespace()
    pvc = client.read_namespaced_persistent_volume_claim(pvc_name, namespace=namespace)

    # Update the PVC to request more storage
    pvc.spec.resources.requests["storage"] = size
    new_pvc=client.patch_namespaced_persistent_volume_claim(pvc_name, namespace, pvc)
    return new_pvc

def rewrite_data_passing(workflow: dict, volume: dict, path_prefix: str = 'artifact_data/') -> dict:
    workflow = copy.deepcopy(workflow)
    templates = workflow['spec']['templates']

    container_templates = [template for template in templates if 'container' in template]
    dag_templates = [template for template in templates if 'dag' in template]
    steps_templates = [template for template in templates if 'steps' in template]

    execution_data_dir = path_prefix + '{{workflow.uid}}_{{pod.name}}/'

    data_volume_name = 'data-storage'
    volume['name'] = data_volume_name

    subpath_parameter_name_suffix = '-subpath'

    def convert_artifact_reference_to_parameter_reference(reference: str) -> str:
        parameter_reference = re.sub(
            r'{{([^}]+)\.artifacts\.([^}]+)}}',
            r'{{\1.parameters.\2' + subpath_parameter_name_suffix + '}}', # re.escape(subpath_parameter_name_suffix) escapes too much.
            reference,
        )
        return parameter_reference

    # Adding the data storage volume to the workflow
    workflow['spec'].setdefault('volumes', []).append(volume)

    all_artifact_file_names = set() # All artifacts should have same file name (usually, "data"). This variable holds all different artifact names for verification.

    # Rewriting container templates
    for template in templates:
        if 'container' not in template and 'script' not in template:
            continue
        container_spec = template['container'] or template['steps']
        # Inputs
        input_artifacts = template.get('inputs', {}).get('artifacts', [])
        if input_artifacts:
            input_parameters = template.setdefault('inputs', {}).setdefault('parameters', [])
            volume_mounts = container_spec.setdefault('volumeMounts', [])
            for input_artifact in input_artifacts:
                subpath_parameter_name = input_artifact['name'] + subpath_parameter_name_suffix # TODO: Maybe handle clashing names.
                artifact_file_name = os.path.basename(input_artifact['path'])
                all_artifact_file_names.add(artifact_file_name)
                artifact_dir = os.path.dirname(input_artifact['path'])
                volume_mounts.append({
                    'mountPath': artifact_dir,
                    'name': data_volume_name,
                    'subPath': '{{inputs.parameters.' + subpath_parameter_name + '}}',
                    'readOnly': True,
                })
                input_parameters.append({
                    'name': subpath_parameter_name,
                })
        # template.get('inputs', {}).pop('artifacts', None)

        # Outputs
        output_artifacts = template.get('outputs', {}).get('artifacts', [])
        if output_artifacts:
            output_parameters = template.setdefault('outputs', {}).setdefault('parameters', [])
            del template.get('outputs', {})['artifacts']
            volume_mounts = container_spec.setdefault('volumeMounts', [])
            for output_artifact in output_artifacts:
                output_name = output_artifact['name']
                subpath_parameter_name = output_name + subpath_parameter_name_suffix # TODO: Maybe handle clashing names.
                artifact_file_name = os.path.basename(output_artifact['path'])
                all_artifact_file_names.add(artifact_file_name)
                artifact_dir = os.path.dirname(output_artifact['path'])
                output_subpath = execution_data_dir + output_name
                volume_mounts.append({
                    'mountPath': artifact_dir,
                    'name': data_volume_name,
                    'subPath': output_subpath, # TODO: Switch to subPathExpr when it's out of beta: https://kubernetes.io/docs/concepts/storage/volumes/#using-subpath-with-expanded-environment-variables
                })
                output_parameters.append({
                    'name': subpath_parameter_name,
                    'value': output_subpath, # Requires Argo 2.3.0+
                })
            # template.get('outputs', {}).pop('artifacts', None)

    # Rewrite DAG templates
    for template in templates:
        if 'dag' not in template and 'steps' not in template:
            continue

        # Inputs
        input_artifacts = template.get('inputs', {}).get('artifacts', [])
        if input_artifacts:
            input_parameters = template.setdefault('inputs', {}).setdefault('parameters', [])
            volume_mounts = container_spec.setdefault('volumeMounts', [])
            for input_artifact in input_artifacts:
                subpath_parameter_name = input_artifact['name'] + subpath_parameter_name_suffix # TODO: Maybe handle clashing names.
                input_parameters.append({
                    'name': subpath_parameter_name,
                })
        # template.get('inputs', {}).pop('artifacts', None)

        # Outputs
        output_artifacts = template.get('outputs', {}).get('artifacts', [])
        if output_artifacts:
            output_parameters = template.setdefault('outputs', {}).setdefault('parameters', [])
            volume_mounts = container_spec.setdefault('volumeMounts', [])
            for output_artifact in output_artifacts:
                output_name = output_artifact['name']
                subpath_parameter_name = output_name + subpath_parameter_name_suffix # TODO: Maybe handle clashing names.
                output_parameters.append({
                    'name': subpath_parameter_name,
                    'valueFrom': {
                        'parameter': convert_artifact_reference_to_parameter_reference(output_artifact['from'])
                    },
                })
        # template.get('outputs', {}).pop('artifacts', None)
        
        # Arguments
        for task in template.get('dag', {}).get('tasks', []) + [steps for group in template.get('steps', []) for steps in group]:
            argument_artifacts = task.get('arguments', {}).get('artifacts', [])
            if argument_artifacts:
                argument_parameters = task.setdefault('arguments', {}).setdefault('parameters', [])
                for argument_artifact in argument_artifacts:
                    if 'from' not in argument_artifact:
                        raise NotImplementedError('Volume-based data passing rewriter does not support constant artifact arguments at this moment. Only references can be passed.')
                    subpath_parameter_name = argument_artifact['name'] + subpath_parameter_name_suffix # TODO: Maybe handle clashing names.
                    argument_parameters.append({
                        'name': subpath_parameter_name,
                        'value': convert_artifact_reference_to_parameter_reference(argument_artifact['from']),
                    })
            # task.get('arguments', {}).pop('artifacts', None)

        # There should not be any artifact references in any other part of DAG template (only parameter references)

    # Check that all artifacts have the same file names
    if len(all_artifact_file_names) > 1:
        warnings.warn('Detected different artifact file names: [{}]. The workflow can fail at runtime. Please use the same file name (e.g. "data") for all artifacts.'.format(', '.join(all_artifact_file_names)))

    # Fail if workflow has argument artifacts
    workflow_argument_artifacts = workflow['spec'].get('arguments', {}).get('artifacts', [])
    if workflow_argument_artifacts:
        raise NotImplementedError('Volume-based data passing rewriter does not support constant artifact arguments at this moment. Only references can be passed.')

    return workflow

def transform_workflow(workflow: dict, volume:dict, path_prefix: str = 'artifact_data/') -> dict:
    if isinstance(volume, dict):
        volume_dict = volume
    else:
        volume_dict = kubernetes.kubernetes.client.ApiClient(
        ).sanitize_for_serialization(volume)
    return rewrite_data_passing(workflow, volume_dict,
                                                   path_prefix)
