#!/bin/bash
docker login -u="$ARTIFACTORY_USERNAME" -p="$ARTIFACTORY_PASSWORD" docker.internal.sysdig.com
IMAGE_ID=$(docker images | grep ^quay.io/freshtracks.io/avalanche | tr -s " " | cut -d " " -f3 )
echo $IMAGE_ID
docker tag ${IMAGE_ID} docker.internal.sysdig.com/avalanche:prom-beacon
docker push docker.internal.sysdig.com/avalanche:prom-beacon
