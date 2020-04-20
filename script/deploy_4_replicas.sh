#!/bin/bash
set -euxo pipefail

cd ../
\cp -rf ./config ./target/
mvn -T4 package
scp -r -P 19122 "target/config" "target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar" apl@223.194.70.105:~/
scp -r -P 19222 "target/config" "target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar" apl@223.194.70.105:~/
scp -r -P 19322 "target/config" "target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar" apl@223.194.70.105:~/
scp -r -P 19422 "target/config" "target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar" apl@223.194.70.105:~/

tmux -2 new -s PBFT \; \
  send-keys "sed 's/IP/223.194.70.105/' ./script/deploy_4_replicas.sh | sed 's/PORT/19119/'| sed 's/VIRTUALPORT/19030/'| sed -n '25,\$p' | ssh -p 19122 apl@223.194.70.105 \" bash -s \" " C-m \; \
  split-window -v -p 50 \; \
  send-keys "sed 's/IP/223.194.70.105/' ./script/deploy_4_replicas.sh | sed 's/PORT/19219/'| sed 's/VIRTUALPORT/19030/'| sed -n '25,\$p' | ssh -p 19222 apl@223.194.70.105 \" bash -s \" " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/223.194.70.105/' ./script/deploy_4_replicas.sh | sed 's/PORT/19319/'| sed 's/VIRTUALPORT/19030/'| sed -n '25,\$p' | ssh -p 19322 apl@223.194.70.105 \" bash -s \" " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/223.194.70.105/' ./script/deploy_4_replicas.sh | sed 's/PORT/19419/'| sed 's/VIRTUALPORT/19030/'| sed -n '25,\$p' | ssh -p 19422 apl@223.194.70.105 \" bash -s \" " C-m \; \
  select-layout tiled
exit



source /etc/profile
rm -rf ~/replicaData_IP_PORT
pkill -9 -f "java -jar"
java -jar ~/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar server IP PORT VIRTUALPORT