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

def fetch_data(
    project_id: str,
    input_bq_table: str,
    gcs_bucket: str,
    gcs_path: str
):
    """
    Fetch data from BQ 

    Args:
        project_id: the id of project.
        input_bq_table: the input table in bq.
        gcs_bucket: the gcs buckt name.
        gcs_path: the gcs path.

    """

    import pandas as pd
    import google.auth
    from google.cloud import aiplatform
    from google.cloud import storage

    aiplatform.init(project=project_id)
    credentials, project = google.auth.default()

    def read_bq(project_id,table_name):
        sql = f"SELECT * FROM {table_name} limit 10"
        df = pd.read_gbq(sql, project_id=project_id, dialect="standard",credentials=credentials)  
        return df

    def save_to_gcs(df, bucket_name, blob_name):
        """Write and read a blob from GCS using file-like IO"""

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(df.to_csv(), 'text/csv')

    print(f'Reading input bq table: {input_bq_table}')
    # df = read_bq(project_id,input_bq_table)
    df = pd.DataFrame({'movieId':[1,2,3],
                  'title': ['Tangled (2010)', 'Rubber (2010)','Toy Story 3 (2010)'], 
                  'genres': ['Animation|Children|Comedy|Fantasy|Musical|Romance|IMAX', 'Action|Adventure|Comedy|Crime|Drama|Film-Noir|Horror|Mystery|Thriller|Western','Adventure|Animation|Children|Comedy|Fantasy|IMAX']})
    df.insert(0, 'prompt_id', range(0, 0 + len(df)))
    df['title'] = df['title'].apply(lambda s: s.split(" (")[0])
    save_to_gcs(df, gcs_bucket, gcs_path)

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
