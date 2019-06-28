#!/bin/bash

mvn package && \

scp -r ./target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar apl@223.194.70.105:~/PBFT/target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar && \

ssh -f  apl@223.194.70.105 '\
	cd PBFT
	rm -f *.db
	pkill --signal 9 -f "java -jar PBFT"
	java -jar target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar server 223.194.70.105 19030 2>&1 &
	for i in $(echo "19031 19032 19033")
	do
		java -jar target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar server 223.194.70.105 $i 2>&1 > /dev/null &
	done'
