#!/bin/bash

set -euo pipefail

function mc 
{
  port=$1; shift
  command="$@"
  
  docker run -it --net=host --entrypoint=/bin/sh minio/mc -c "/usr/bin/mc -q --insecure config host add s3 https://localhost:$port/ minio minio123; /usr/bin/mc -q --insecure $command"
}

mc 9901 mb s3/from-bucket
mc 9901 mb s3/to-bucket
mc 9902 mb s3/to-bucket
mc 9902 mb s3/from-bucket
mc 9903 mb --region us-east-1 s3/to-bucket
mc 9903 mb --region us-east-1 s3/from-bucket
