#!/bin/bash
source ./adEnv/bin/activate
depicorn -w 1 -k uvicorn.workers.UvicornWorker -b 'unix:/tmp/deployment.sock' --forwarded-allow-ips="*" api_entry:incoming &
disown
deactivate