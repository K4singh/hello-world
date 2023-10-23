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

def call_llm_model(
    project_id: str,
    llm_model:str,
    output_bq_table: str,
    gcs_bucket:str , 
    input_path:str,
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

    def llm_prompt(genre,movie):
        return """You are movie critic, Rate the %s movie %s based on your knowledge on scale of 1-5, with 1 being the lowest and 5 being the best. reply just score in format x/5 """ % (genre,movie)

    def save_to_gcs(df, bucket_name, blob_name):
        """Write and read a blob from GCS using file-like IO"""

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(df.to_csv(), 'text/csv')


    # @sleep_and_retry
    # @limits(calls=60, period=60)
    def get_rating(project_id, genre, movie, llm_model):
        vertexai.init(project=project_id, location="us-central1")
        parameters = {
            "max_output_tokens": 256,
            "temperature": 0.1,
            "top_p": 0.5,
            "top_k": 20
        }
        model = TextGenerationModel.from_pretrained(llm_model)
        try:

            prompt = llm_prompt(genre, movie)

            response = model.predict(
                prompt,
                **parameters
            )
            return response.text.split('\n\n')[0]
        except:
            return  ''


    print(f'Reading data from bucket: {gcs_bucket}')
    df = read_csv_from_gcs(gcs_bucket, input_path)

    prompt_set_df = df[['prompt_id','genres','title']]
    prompt_tuple = [tuple(x) for x in prompt_set_df.to_numpy()]

    print('Calling bison model')
    def helper(prompt_arguments, model_type):
        return get_rating(prompt_arguments[0], prompt_arguments[1], prompt_arguments[2], model_type)

    for index, row in df.iterrows():
        df.at[index, 'rating'] = get_rating(project_id,row.genres,row.title,llm_model)

    # with PoolExecutor(max_workers=10) as executor:
    #     output = list(executor.map(helper, prompt_tuple, itertools.repeat(llm_model, len(prompt_tuple))))

    # batch_result_df = pd.DataFrame(output, columns=['prompt_id','rating'])
    # df = df.merge(batch_result_df,how='left',on='prompt_id')
    save_to_gcs(df, gcs_bucket, output_path)

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
