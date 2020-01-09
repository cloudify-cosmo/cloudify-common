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
        PLUGIN_PATH=$(PWD)
        if [ "$PLUGIN_TAG_NAME" == "master" ];then
            git checkout master
        else
            git checkout -b $PLUGIN_TAG_NAME origin/$PLUGIN_TAG_NAME
        fi
    popd
    # This will generate a wagon file and dump it to the current plugin name
    # directory, this should work for all linux image but for Redhat we need
    # to build it locally since it needs subscription account
    echo "## echo build wagon package using docker"
    echo "git clone https://github.com/$GITHUB_ORGANIZATION/$PLUGIN_NAME.git"
    git clone https://$GITHUB_USERNAME:$GITHUB_TOKEN@github.com/$GITHUB_ORGANIZATION/$WAGON_BUILDLER_REPO.git
    IMAGE_NAME="cloudify-$PLUGIN_PLATFORM-wagon-builder"
    pushd $WAGON_BUILDLER_REPO
         git checkout $WAGON_BUILDLER_BRANCH
         pushd $PLUGIN_PLATFORM
             if [$PLUGIN_PLATFORM == redhat*]; then
                  docker build -t $IMAGE_NAME --build-arg USERNAME=$REL_SUB_USERNAME --build-arg PASSWORD=$REL_SUB_PASSWORD .
             else
                 docker build -t $IMAGE_NAME .
             fi
         popd
    popd

    # This will generate/dump the wagon file inside the Plugin directory
    docker run -v $PLUGIN_PATH:/packaging $IMAGE_NAME
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

if [ "$#" -eq 9 ]; then
  CONSTRAINTS_FILE=$9
  echo "There are 9 params"

else
  echo "There are 13 params"
  PLUGIN_PLATFORM=$9
  WAGON_BUILDLER_REPO=${10}
  WAGON_BUILDLER_BRANCH=${11}
  REL_SUB_USERNAME=${12}
  REL_SUB_PASSWORD=${13}
fi

export AWS_S3_BUCKET="cloudify-release-eu"
export AWS_S3_PATH="cloudify/wagons/$PLUGIN_NAME/$PLUGIN_S3_FOLDER"

print_plugins_params
wagon_create_package &&
cd $PLUGIN_NAME &&
create_md5 "wgn" &&
[ -z ${AWS_ACCESS_KEY} ] || upload_to_s3 "wgn" && upload_to_s3 "md5"

