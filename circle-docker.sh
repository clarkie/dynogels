#!/usr/bin/env bash
set -e
set -x

docker pull jcdietrich/dynamo-local:latest
docker run \
  --name "dynamodb" -d \
  -p "8000:4567" \
  -e AWS_ACCESS_KEY_ID=AKID \
  -e AWS_SECRET_ACCESS_KEY=SECRET \
  -e AWS_REGION=us-east-1 \
  jcdietrich/dynamo-local:latest
