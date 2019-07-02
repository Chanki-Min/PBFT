#!/bin/bash
mvn -T4 package
scp target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.105:~/
scp target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.106:~/
scp target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.107:~/
scp target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.108:~/

tmux -2  new -s PBFT \; \
	 send-keys "sed 's/PORT/19030/' script.sh | sed 's/IP/105/' | sed -n '19,\$p' | ssh apl@223.194.70.105 \" bash -s \" " C-m \; \
     split-window -v -p 50 \; \
	 send-keys "sed 's/PORT/19031/' script.sh | sed 's/IP/106/' | sed -n '19,\$p' | ssh apl@223.194.70.106 \" bash -s \" " C-m \; \
	 split-window -h -p 50 \; \
	 send-keys "sed 's/PORT/19032/' script.sh | sed 's/IP/107/' | sed -n '19,\$p' | ssh apl@223.194.70.107 \" bash -s \" " C-m \; \
	 split-window -h -p 50 \; \
	 send-keys "sed 's/PORT/19033/' script.sh | sed 's/IP/108/' | sed -n '19,\$p' | ssh apl@223.194.70.108 \" bash -s \" " C-m \; \
	 select-layout tiled
exit




source /etc/profile
rm -f ~/*.db  
pkill -9 -f "java -jar" 
sleep 1 
java -jar ~/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar server 223.194.70.IP PORT 
