#!/bin/bash

# usage:
# bash build_and_push.sh <GCP-PROJECT-ID> <IMAGE-NAME> <IMAGE-VERSION>
# ex: bash build_and_push.sh mathem-ml-datahem-test salesforce 0.1.11
# has to be in order
# or no inputs and wait for prompts

set -e

if [ -z "$1" ];
then
    printf "Enter GCP project to use: "
    read input_var1
    export PROJECT=$input_var1
else
    export PROJECT=$1
fi

if [ -z "$2" ];
then
    printf "Enter image name to use: "
    read input_var2
    export IMAGE_NAME=$input_var2
else
    export IMAGE_NAME=$2
fi

if [ -z "$3" ];
then
    printf "Enter image version to use: "
    read input_var3
    export IMAGE_VERSION=$input_var3
else
    export IMAGE_VERSION=$3
fi

export MODULE=pipelines/${IMAGE_NAME}
export REGION=europe-west1
export REPOSITORY=streamprocessor
export BUCKET_NAME=gs://${PROJECT}-${REPOSITORY}
export TARGET_GCR_IMAGE=${REGION}-docker.pkg.dev/${PROJECT}/streamprocessor/${IMAGE_NAME}:${IMAGE_VERSION}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/${MODULE}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec-${IMAGE_VERSION}.json

existing_tags=$(gcloud container images list-tags --filter="tags:$IMAGE_VERSION" --format=json ${REGION}-docker.pkg.dev/${PROJECT}/streamprocessor/${IMAGE_NAME})

if [[ "$existing_tags" == "[]" ]]; then
  printf "\nNew tag: $IMAGE_VERSION"
else
    printf "\n\nTag: $IMAGE_VERSION already exists. Overwrite? y/N "
    read input_var4
    if [ "$input_var4" = "y" ];
    then
        printf "\nOverwriting..."
    else
        printf "\nAborting\n"
        exit 0
    fi
fi

printf "\nRunning with the following variables:"
printf "\n\tPROJECT=$PROJECT"
printf "\n\tTARGET_GCR_IMAGE=$TARGET_GCR_IMAGE"
printf "\n\tBASE_CONTAINER_IMAGE=$BASE_CONTAINER_IMAGE"
printf "\n\tBASE_CONTAINER_IMAGE_VERSION=$BASE_CONTAINER_IMAGE_VERSION"
printf "\n\tAPP_ROOT=$APP_ROOT"
printf "\n\tDATAFLOW_JAVA_COMMAND_SPEC=$DATAFLOW_JAVA_COMMAND_SPEC"
printf "\n\tMODULE=$MODULE"
printf "\n\tTEMPLATE_IMAGE_SPEC=$TEMPLATE_IMAGE_SPEC"

printf "\n\nContinue? y/N "
read input_var4
if [ "$input_var4" = "y" ];
then
    printf "\nStarting deploy..."
else
    printf "\nAborting\n"
    exit 0
fi


printf "\nSetting GCP project..."
gcloud config set project ${PROJECT}

printf "\nBuilding package with maven and pushing docker image to artifact registry..."
mvn clean package \
-Dimage=${TARGET_GCR_IMAGE} \
-Dbase-container-image=${BASE_CONTAINER_IMAGE} \
-Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
-Dapp-root=${APP_ROOT} \
-Dcommand-spec=${DATAFLOW_JAVA_COMMAND_SPEC} \
-am -pl ${MODULE}

printf "\nUploading new image spec to cloud storage..."
$(cat ${MODULE}/image-spec.json | sed "s|ARTIFACT_REGISTRY|${TARGET_GCR_IMAGE}|g" > image_spec.json)
gsutil cp image_spec.json ${TEMPLATE_IMAGE_SPEC}
rm image_spec.json

printf "\nFinished deploying!\n"
