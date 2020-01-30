#!/bin/bash
set -euxo pipefail

cd ../
cp -r ./config ./target/
mvn -T4 package ; set +euxo pipefail
pkill -9 -f "java -jar" ; set -euxo pipefail

tmux -2 new -s PBFT \; \
  send-keys "sed 's/IP/127.0.0.1/' ./script/local_7_replicas.sh | sed 's/PORT/19119/'| sed 's/VIRTUALPORT/19119/'| sed -n '30,\$p' | bash -s " C-m \; \
  split-window -v -p 50 \; \
  send-keys "sed 's/IP/127.0.0.1/' ./script/local_7_replicas.sh | sed 's/PORT/19219/'| sed 's/VIRTUALPORT/19219/'| sed -n '30,\$p' | bash -s " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/127.0.0.1/' ./script/local_7_replicas.sh | sed 's/PORT/19319/'| sed 's/VIRTUALPORT/19319/'| sed -n '30,\$p' | bash -s " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/127.0.0.1/' ./script/local_7_replicas.sh | sed 's/PORT/19419/'| sed 's/VIRTUALPORT/19419/'| sed -n '30,\$p' | bash -s " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/127.0.0.1/' ./script/local_7_replicas.sh | sed 's/PORT/19519/'| sed 's/VIRTUALPORT/19519/'| sed -n '30,\$p' | bash -s " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/127.0.0.1/' ./script/local_7_replicas.sh | sed 's/PORT/19619/'| sed 's/VIRTUALPORT/19619/'| sed -n '30,\$p' | bash -s " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/127.0.0.1/' ./script/local_7_replicas.sh | sed 's/PORT/19719/'| sed 's/VIRTUALPORT/19719/'| sed -n '30,\$p' | bash -s " C-m \; \
  select-layout tiled
exit





source /etc/profile
rm -rf target/replicaData_IP_PORT
java -jar target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar server IP PORT VIRTUALPORT