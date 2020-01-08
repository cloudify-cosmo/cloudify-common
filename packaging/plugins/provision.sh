#!/bin/bash -xe

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

function wagon_create_package(){

    echo "## wagon create package"
    echo "git clone https://github.com/$GITHUB_ORGANIZATION/$PLUGIN_NAME.git"
    git clone https://$GITHUB_USERNAME:$GITHUB_TOKEN@github.com/$GITHUB_ORGANIZATION/$PLUGIN_NAME.git
    pushd $PLUGIN_NAME
        if [ "$PLUGIN_TAG_NAME" == "master" ];then
            git checkout master
        else
            git checkout -b $PLUGIN_TAG_NAME origin/$PLUGIN_TAG_NAME
        fi
    popd
    # This will generate a wagon file and dump it to the current plugin name
    # directory, this should work for all linux image but for Redhat we need
    # to build it locally since it needs subscription account
    #TODO need to handle Redhat
    docker run -v $PLUGIN_NAME:/packaging $DOCKER_IMAGE
}



# VERSION/PRERELEASE/BUILD must be exported as they is being read as an env var by the cloudify-agent-packager
export CORE_TAG_NAME="5.1.0.dev1"
export CORE_BRANCH="master"
curl https://raw.githubusercontent.com/cloudify-cosmo/cloudify-common/$CORE_BRANCH/packaging/common/provision.sh -o ./common-provision.sh &&
source common-provision.sh


GITHUB_USERNAME=$1
GITHUB_TOKEN=$2
AWS_ACCESS_KEY_ID=$3
AWS_ACCESS_KEY=$4
PLUGIN_NAME=$5
PLUGIN_TAG_NAME=$6
PLUGIN_S3_FOLDER=$7
GITHUB_ORGANIZATION=$8
CONSTRAINTS_FILE=$9
DOCKER_IMAGE=${10}

export AWS_S3_BUCKET="cloudify-release-eu"
export AWS_S3_PATH="cloudify/wagons/$PLUGIN_NAME/$PLUGIN_S3_FOLDER"

print_plugins_params
wagon_create_package &&
cd $PLUGIN_NAME &&
create_md5 "wgn" &&
[ -z ${AWS_ACCESS_KEY} ] || upload_to_s3 "wgn" && upload_to_s3 "md5"

