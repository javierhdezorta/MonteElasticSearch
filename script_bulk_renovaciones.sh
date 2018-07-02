#!/bin/bash

curl --user BPMrenovaciones:123456Nmp -XPOST 'https://0f015a81ee05196b255efee81a128f42.us-east-1.aws.found.io:9243/_bulk?pretty&refresh' -H "Content-Type: application/x-ndjson" --data-binary @"elastic_renovaciones.json"
