# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# DISCLAIMER: This code is generated as part of the AutoMLOps output.

"""Kubeflow Pipeline Definition"""

import argparse
from typing import *
import os

from google.cloud import storage
import kfp
from kfp.v2 import compiler, dsl
from kfp.v2.dsl import *
import yaml

def upload_pipeline_spec(gs_pipeline_job_spec_path: str,
                         pipeline_job_spec_path: str,
                         storage_bucket_name: str):
    '''Upload pipeline job spec from local to GCS'''
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(storage_bucket_name)
    filename = '/'.join(gs_pipeline_job_spec_path.split('/')[3:])
    blob = bucket.blob(filename)
    blob.upload_from_filename(pipeline_job_spec_path)

def load_custom_component(component_name: str):
    component_path = os.path.join('components',
                                component_name,
                              'component.yaml')
    return kfp.components.load_component_from_file(component_path)

def create_training_pipeline(pipeline_job_spec_path: str):
    call_llm_model = load_custom_component(component_name='call_llm_model')
    fetch_data = load_custom_component(component_name='fetch_data')
    post_processing_and_export_to_bq = load_custom_component(component_name='post_processing_and_export_to_bq')

    @dsl.pipeline(
        name='movie-rating-pipeline',
        description='calling llm model api to get movie rating',
    )
    def pipeline(project_id: str,
                 llm_model:str,
                 input_bq_table: str,
                 output_bq_table: str,
                 gcs_bucket:str,
                 gcs_path:str,
                 llm_output_path:str
                ):

        if llm_model not in ['text-bison@001','text-bison']:
            raise ValueError("llm model is not valid")

        read_data = fetch_data(
                        project_id = project_id,
                        input_bq_table=input_bq_table,
                        gcs_bucket= gcs_bucket,
                        gcs_path = gcs_path)

        call_llm_prompt = call_llm_model(
                        project_id = project_id,
                        llm_model = llm_model,
                        output_bq_table = output_bq_table,
                        gcs_bucket=gcs_bucket,
                        input_path=gcs_path,
                        output_path=llm_output_path
                        ).after(read_data)

        export_to_bq = post_processing_and_export_to_bq(
                        project_id = project_id,
                        output_bq_table = output_bq_table,
                        gcs_bucket=gcs_bucket,
                        output_path=llm_output_path
                        ).after(call_llm_prompt)

    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=pipeline_job_spec_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str,
                       help='The config file for setting default values.')

    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    create_training_pipeline(
        pipeline_job_spec_path=config['pipelines']['pipeline_job_spec_path'])

    upload_pipeline_spec(
        gs_pipeline_job_spec_path=config['pipelines']['gs_pipeline_job_spec_path'],
        pipeline_job_spec_path=config['pipelines']['pipeline_job_spec_path'],
        storage_bucket_name=config['gcp']['storage_bucket_name'])
