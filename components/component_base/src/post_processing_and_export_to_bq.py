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

import argparse
import json
from kfp.v2.components import executor

import kfp
from kfp.v2 import dsl
from kfp.v2.dsl import *
from typing import *

def post_processing_and_export_to_bq(
    project_id: str,
    output_bq_table: str,
    gcs_bucket:str , 
    output_path:str
):
    """
    concurrent llm calls to generate movie rating based on genre
    """

    import pandas as pd
    from ratelimit import limits,sleep_and_retry
    from concurrent.futures import ThreadPoolExecutor as PoolExecutor
    import itertools
    from io import BytesIO

    import vertexai
    from vertexai.language_models import TextGenerationModel

    from google.cloud import storage
    from google.cloud import aiplatform
    import google.auth

    credentials, project = google.auth.default()
    aiplatform.init(project=project_id)

    def read_csv_from_gcs(bucket_name, blob_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        binary_stream = blob.download_as_string()
        return pd.read_csv(BytesIO(binary_stream))

    print(f'Reading data from bucket: {gcs_bucket}')
    df = read_csv_from_gcs(gcs_bucket, output_path)

    df = df[['prompt_id','movieId','genres','title','rating']]
    df['rating']= df['rating'].apply(lambda s: str(eval(s)))
    print("Saving results in BQ...")
    df.to_gbq(output_bq_table, project_id=project_id,if_exists='replace')  

def main():
    """Main executor."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--executor_input', type=str)
    parser.add_argument('--function_to_execute', type=str)

    args, _ = parser.parse_known_args()
    executor_input = json.loads(args.executor_input)
    function_to_execute = globals()[args.function_to_execute]

    executor.Executor(
        executor_input=executor_input,
        function_to_execute=function_to_execute).execute()

if __name__ == '__main__':
    main()
