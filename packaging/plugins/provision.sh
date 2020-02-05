#!/bin/bash

set -xe

SUDO=""

function print_plugins_params() {

    echo "## print common parameters"

    declare -A params=( ["PLUGIN_NAME"]=$PLUGIN_NAME ["PLUGIN_TAG_NAME"]=$PLUGIN_TAG_NAME  \
                        ["PLUGIN_S3_FOLDER"]=$PLUGIN_S3_FOLDER )
    for param in "${!params[@]}"
    do
            echo "$param - ${params["$param"]}"
    done
}

function install_windows_dependencies(){
    # Prepare virtualenv in windows machine
    echo "## Installing necessary dependencies"
    pip install virtualenv==15.1.0

    # Prepare pip & wagon in windows machine
    echo "## installing wagon"
    virtualenv env
    source env/bin/activate
    curl https://bootstrap.pypa.io/get-pip.py | python
    pip install --upgrade pip==9.0.1 setuptools
    pip install wagon==0.3.2
}

function checkout_plugin() {
    echo "git clone https://github.com/$GITHUB_ORGANIZATION/$PLUGIN_NAME.git"
    git clone https://$GITHUB_USERNAME:$GITHUB_TOKEN@github.com/$GITHUB_ORGANIZATION/$PLUGIN_NAME.git
    pushd $PLUGIN_NAME
        PLUGIN_PATH=$(pwd)
        if [ "$PLUGIN_TAG_NAME" == "master" ];then
            git checkout master
        else
            git checkout -b $PLUGIN_TAG_NAME origin/$PLUGIN_TAG_NAME
        fi
    popd

}

function create_windows_wagon_package() {
    echo "## Generate Wagon Package on Windows"
    # Checkout to the plugin
    checkout_plugin

    echo "manylinux1_compatible = False" > "env/bin/_manylinux.py"
    mkdir create_wagon ; cd create_wagon
    if [ ! -z "$CONSTRAINTS_FILE" ] && [ -f "/packaging/$CONSTRAINTS_FILE" ];then
        echo "## /packaging/$CONSTRAINTS_FILE exist"
        wagon create -s ../$PLUGIN_NAME/ -r -v -f -a '--no-cache-dir -c /packaging/'$CONSTRAINTS_FILE''
    else
        echo "## /packaging/$CONSTRAINTS_FILE doesn't exist"
        wagon create -s ../$PLUGIN_NAME/ -r -v -f
    fi

}

function generate_windows_plugin() {
    # Print Plugin params
    print_plugins_params &&
    # Install common prerequisite
    install_common_prereqs &&
    # Install windows dependencies
    install_windows_dependencies &&
    # create_window_wagon
    create_windows_wagon_package
}

function override_constraints_file() {
    if [ ! -z "$CONSTRAINTS_FILE" ];then
        cp -rf $CONSTRAINTS_FILE $PLUGIN_PATH/
    fi
}

function clean_docker_resources() {
  # This will clean all exited docker containers
  docker rm $CONTAINER_NAME

  # Remove Docker image
  docker rmi $IMAGE_NAME --force
}

function wagon_create_package(){

    echo "## wagon create package"

    checkout_plugin

    # Check if constraint file is provided or not
    override_constraints_file

    # This will generate a wagon file and dump it to the current plugin name
    # directory, this should work for all linux image but for Redhat we need
    # to build it locally since it needs subscription account
    echo "## build wagon package using docker"
    git clone $WAGON_BUILDLER_REPO
    # Get the name of the wagon builder project to navigate inside it
    WAGON_BUILDLER=$(basename $WAGON_BUILDLER_REPO | cut -f1 -d".")
    # Start building docker image inside WAGON_BUILDLER
    pushd $WAGON_BUILDLER
         git checkout $WAGON_BUILDLER_BRANCH
         pushd $PLUGIN_PLATFORM
             # Replace "_" with "-" in order to generate name for the
             # docker image contains "-" instead of "_"
             PLUGIN_PLATFORM=$(sed "s/_/-/g" <<< $PLUGIN_PLATFORM)
             IMAGE_NAME="cloudify-$PLUGIN_PLATFORM-wagon-builder"
             CONTAINER_NAME=$IMAGE_NAME
             if [[ $PLUGIN_PLATFORM == "redhat"* ]]; then
                  docker build -t $IMAGE_NAME --build-arg USERNAME=$REL_SUB_USERNAME --build-arg PASSWORD=$REL_SUB_PASSWORD .
             else
                  IMAGE_NAME=$DOCKER_ACCOUNT/$IMAGE_NAME
             fi
         popd
    popd

    docker run --name $CONTAINER_NAME -v $PLUGIN_PATH:/packaging $IMAGE_NAME
    echo "## build wagon package created successfully"
}

# VERSION/PRERELEASE/BUILD must be exported as they is being read as an env var by the cloudify-agent-packager
export CORE_TAG_NAME="5.1.0.dev1"
export CORE_BRANCH="master"
curl https://raw.githubusercontent.com/cloudify-cosmo/cloudify-common/$CORE_BRANCH/packaging/common/provision.sh -o ./common-provision.sh &&
source common-provision.sh

# These are common inputs for both building wagon using docker for (Linux)
# And for building wagong in windows using vagrant
GITHUB_USERNAME=$1
GITHUB_TOKEN=$2
AWS_ACCESS_KEY_ID=$3
AWS_ACCESS_KEY=$4
PLUGIN_NAME=$5
PLUGIN_TAG_NAME=$6
PLUGIN_S3_FOLDER=$7
GITHUB_ORGANIZATION=$8
CONSTRAINTS_FILE=$9

DOCKER_BUILDER=false

if [ "$#" -gt 9 ]; then
    echo "# Docker Builder is running....."
    PLUGIN_PLATFORM=${10}
    WAGON_BUILDLER_REPO=${11}
    WAGON_BUILDLER_BRANCH=${12}
    DOCKER_ACCOUNT=${13}
    REL_SUB_USERNAME=${14}
    REL_SUB_PASSWORD=${15}
    DOCKER_BUILDER=true
fi

export AWS_S3_BUCKET="cloudify-release-eu"
export AWS_S3_PATH="cloudify/wagons/$PLUGIN_NAME/$PLUGIN_S3_FOLDER"

if $DOCKER_BUILDER; then
  print_plugins_params &&
  wagon_create_package &&
  cd $PLUGIN_NAME &&
  create_md5 "wgn" &&
  [ -z ${AWS_ACCESS_KEY} ] || upload_to_s3 "wgn" && upload_to_s3 "md5" &&
  clean_docker_resources
else
   generate_windows_plugin &&
   create_md5 "wgn" &&
   [ -z ${AWS_ACCESS_KEY} ] || upload_to_s3 "wgn" && upload_to_s3 "md5"
fi

