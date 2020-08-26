#!/usr/bin/env bash
#   Use this script to test if a given TCP host/port are available

while true; do
  status=`curl -LI http://localhost:8083 -o /dev/null -w '%{http_code}\n' -s`
  if [[ ${status} == "200" ]]; then
    echo "connected to schema-registry localhost:8083"
    break
  fi
  echo "waiting for schema-registry localhost:8083"
  sleep 15s
done


# Log the docker logs
docker-compose -f integration-test/docker-compose.yml logs --follow  > target/DockerLogs.txt &
