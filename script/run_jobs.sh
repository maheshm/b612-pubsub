#!/bin/bash

#This is required for JAVA app as well
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials/file.json"

while getopts m:n: option
do
    case "${option}"
    in
        m) m=${OPTARG};;
        n) n=${OPTARG};;
    esac
done

#This script expects gcloud to be set up
gcloud init
gcloud pubsub topics create my-topic my-topic-resp
gcloud pubsub subscriptions create --topic my-topic my-sub
gcloud pubsub subscriptions create --topic my-topic-resp my-sub

#run java
./start_publisher.sh $n
./start_subscribers.sh $m

#cleanup
gcloud pubsub topics delete my-topic my-topic-resp
#This wont work, need to get FQN of subscription
gcloud pubsub subscriptions delete my-sub
gcloud pubsub subscriptions delete my-sub
