#!/bin/bash
mvn -T4 package
scp -P 51122 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.111:~/
scp -P 51222 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.111:~/
scp -P 51322 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.111:~/
scp -P 51422 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.111:~/
scp -P 12222 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.111:~/
scp -P 12322 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.111:~/
scp -P 12422 target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.111:~/

tmux -2 new -s PBFT \; \
  send-keys "sed 's/IP/223.194.70.111/' deploy_7_replicas.sh | sed 's/PORT/51119/'| sed 's/VIRTUALPORT/19030/'| sed -n '30,\$p' | ssh -p 51122 apl@223.194.70.111 \" bash -s \" " C-m \; \
  split-window -v -p 50 \; \
  send-keys "sed 's/IP/223.194.70.111/' deploy_7_replicas.sh | sed 's/PORT/51219/'| sed 's/VIRTUALPORT/19030/'| sed -n '30,\$p' | ssh -p 51222 apl@223.194.70.111 \" bash -s \" " C-m \; \
  split-window -v -p 50 \; \
  send-keys "sed 's/IP/223.194.70.111/' deploy_7_replicas.sh | sed 's/PORT/51319/'| sed 's/VIRTUALPORT/19030/'| sed -n '30,\$p' | ssh -p 51322 apl@223.194.70.111 \" bash -s \" " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/223.194.70.111/' deploy_7_replicas.sh | sed 's/PORT/51419/'| sed 's/VIRTUALPORT/19030/'| sed -n '30,\$p' | ssh -p 51422 apl@223.194.70.111 \" bash -s \" " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/223.194.70.111/' deploy_7_replicas.sh | sed 's/PORT/51519/'| sed 's/VIRTUALPORT/19030/'| sed -n '30,\$p' | ssh -p 12222 apl@223.194.70.111 \" bash -s \" " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/223.194.70.111/' deploy_7_replicas.sh | sed 's/PORT/51619/'| sed 's/VIRTUALPORT/19030/'| sed -n '30,\$p' | ssh -p 12322 apl@223.194.70.111 \" bash -s \" " C-m \; \
  split-window -h -p 50 \; \
  send-keys "sed 's/IP/223.194.70.111/' deploy_7_replicas.sh | sed 's/PORT/51719/'| sed 's/VIRTUALPORT/19030/'| sed -n '30,\$p' | ssh -p 12422 apl@223.194.70.111 \" bash -s \" " C-m \; \
  select-layout tiled
exit



source /etc/profile
rm -f ~/*.db
pkill -9 -f "PBFT"
java -jar ~/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar server IP PORT VIRTUALPORT
