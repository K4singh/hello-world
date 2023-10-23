#!/bin/bash 
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

# This script will create an artifact registry and gs bucket if they do not already exist.

GREEN='\033[0;32m'
NC='\033[0m'
ARTIFACT_REPO_LOCATION="us-central1"
ARTIFACT_REPO_NAME="movie-rating-artifact-registry"
BASE_DIR="AutoMLOps/"
BUILD_TRIGGER_LOCATION="us-central1"
BUILD_TRIGGER_NAME="movie-rating-build-trigger"
PIPELINE_JOB_SUBMISSION_SERVICE_IMAGE="us-central1-docker.pkg.dev/tester-sandbox/movie-rating-artifact-registry/movie-rating/submission_service:latest"
PIPELINE_JOB_SUBMISSION_SERVICE_LOCATION="us-central1"
PIPELINE_JOB_SUBMISSION_SERVICE_NAME="movie-rating-job-submission-svc"
PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_SHORT="vertex-pipelines"
PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG="vertex-pipelines@tester-sandbox.iam.gserviceaccount.com"
PROJECT_ID="tester-sandbox"
PUBSUB_TOPIC_NAME="movie-rating-queueing-svc"
SCHEDULE_NAME="movie-rating-schedule"
SCHEDULE_PATTERN="No Schedule Specified"
SCHEDULE_LOCATION="us-central1"
SOURCE_REPO_NAME="K4singh/hello-world"
SOURCE_REPO_BRANCH="automlops"
STORAGE_BUCKET_NAME="tester-sandbox-movie-rating-bucket"
STORAGE_BUCKET_LOCATION="us-central1"

echo -e "$GREEN Setting up API services in project $PROJECT_ID $NC"
gcloud services enable \
  compute.googleapis.com \
  iamcredentials.googleapis.com \
  cloudfunctions.googleapis.com \
  aiplatform.googleapis.com \
  cloudresourcemanager.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  pubsub.googleapis.com \

echo -e "$GREEN Setting up Artifact Registry in project $PROJECT_ID $NC"
if ! (gcloud artifacts repositories list --project="$PROJECT_ID" --location=$ARTIFACT_REPO_LOCATION | grep -E "(^|[[:blank:]])$ARTIFACT_REPO_NAME($|[[:blank:]])"); then

  echo "Creating Artifact Registry: ${ARTIFACT_REPO_NAME} in project $PROJECT_ID"
  gcloud artifacts repositories create "$ARTIFACT_REPO_NAME" \
    --repository-format=docker \
    --location=$ARTIFACT_REPO_LOCATION \
    --project="$PROJECT_ID" \
    --description="Artifact Registry ${ARTIFACT_REPO_NAME} in ${ARTIFACT_REPO_LOCATION}." 

else

  echo "Artifact Registry: ${ARTIFACT_REPO_NAME} already exists in project $PROJECT_ID"

fi

echo -e "$GREEN Setting up Storage Bucket in project $PROJECT_ID $NC"
if !(gsutil ls -b gs://$STORAGE_BUCKET_NAME | grep --fixed-strings "$STORAGE_BUCKET_NAME"); then

  echo "Creating GS Bucket: ${STORAGE_BUCKET_NAME} in project $PROJECT_ID"
  gsutil mb -l ${STORAGE_BUCKET_LOCATION} gs://$STORAGE_BUCKET_NAME

else

  echo "GS Bucket: ${STORAGE_BUCKET_NAME} already exists in project $PROJECT_ID"

fi

echo -e "$GREEN Setting up Pipeline Job Runner Service Account in project $PROJECT_ID $NC"
if ! (gcloud iam service-accounts list --project="$PROJECT_ID" | grep -E "(^|[[:blank:]])$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG($|[[:blank:]])"); then

  echo "Creating Service Account: ${PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_SHORT} in project $PROJECT_ID"
  gcloud iam service-accounts create $PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_SHORT \
      --description="For submitting PipelineJobs" \
      --display-name="Pipeline Runner Service Account"
else

  echo "Service Account: ${PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_SHORT} already exists in project $PROJECT_ID"

fi

echo -e "$GREEN Setting up IAM roles for Pipeline Job Runner Service Account in project $PROJECT_ID $NC"
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG" \
    --role="roles/cloudfunctions.admin" \
    --no-user-output-enabled
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG" \
    --role="roles/storage.admin" \
    --no-user-output-enabled
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG" \
    --role="roles/bigquery.dataEditor" \
    --no-user-output-enabled
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG" \
    --role="roles/aiplatform.user" \
    --no-user-output-enabled
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG" \
    --role="roles/iam.serviceAccountUser" \
    --no-user-output-enabled
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG" \
    --role="roles/artifactregistry.reader" \
    --no-user-output-enabled
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG" \
    --role="roles/bigquery.user" \
    --no-user-output-enabled


# Create Pub/Sub Topic
echo -e "$GREEN Setting up Queueing Service in project $PROJECT_ID $NC"
if ! (gcloud pubsub topics list | grep -E "(^|[[:blank:]])projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC_NAME}($|[[:blank:]])"); then

  echo "Creating Pub/Sub Topic: ${PUBSUB_TOPIC_NAME} in project $PROJECT_ID"
  gcloud pubsub topics create $PUBSUB_TOPIC_NAME

else

  echo "Pub/Sub Topic: ${PUBSUB_TOPIC_NAME} already exists in project $PROJECT_ID"

fi

# Deploy Cloud Function
echo -e "$GREEN Deploying Cloud Functions: ${PIPELINE_JOB_SUBMISSION_SERVICE_NAME} in project $PROJECT_ID $NC"
gcloud functions deploy $PIPELINE_JOB_SUBMISSION_SERVICE_NAME \
  --no-allow-unauthenticated \
  --docker-repository="projects/${PROJECT_ID}/locations/${ARTIFACT_REPO_LOCATION}/repositories/${ARTIFACT_REPO_NAME}" \
  --trigger-topic=$PUBSUB_TOPIC_NAME \
  --entry-point=process_request \
  --runtime=python39 \
  --region=$PIPELINE_JOB_SUBMISSION_SERVICE_LOCATION \
  --memory=512MB \
  --timeout=540s \
  --source=${BASE_DIR}services/submission_service \
  --service-account=$PIPELINE_JOB_RUNNER_SERVICE_ACCOUNT_LONG

