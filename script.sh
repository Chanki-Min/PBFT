#!/bin/bash
mvn -T4 package
scp -P 22 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.105:~/
scp -P 22 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.106:~/
scp -P 22 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.107:~/
scp -P 22 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.108:~/

tmux -2 new -s PBFT \; \
  send-keys "sed 's/IP/223.194.70.105/' script.sh | sed 's/PORT/19030/'| sed 's/VIRTUALPORT/19030/'| sed -n '19,\$p' | ssh -p 22 apl@223.194.70.105 \" bash -s \" " C-m \; \
  split-window -v -p 50 \; \
  send-keys "sed 's/IP/223.194.70.106/' script.sh | sed 's/PORT/19031/'| sed 's/VIRTUALPORT/19031/'| sed -n '19,\$p' | ssh -p 22 apl@223.194.70.106 \" bash -s \" " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/223.194.70.107/' script.sh | sed 's/PORT/19032/'| sed 's/VIRTUALPORT/19032/'| sed -n '19,\$p' | ssh -p 22 apl@223.194.70.107 \" bash -s \" " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/223.194.70.108/' script.sh | sed 's/PORT/19033/'| sed 's/VIRTUALPORT/19033/'| sed -n '19,\$p' | ssh -p 22 apl@223.194.70.108 \" bash -s \" " C-m \; \
  select-layout tiled
exit

source /etc/profile
rm -f ~/*.db
pkill -9 -f "java -jar"
java -jar ~/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar server IP PORT VIRTUALPORT
