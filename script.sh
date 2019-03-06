#!/bin/bash

scp -r src/* apl@223.194.70.127:~/PBFT/src/

ssh -f  apl@223.194.70.127 '\
	cd PBFT
	rm -f *.db
	pkill java
	mvn package
	for i in $(echo "19030 19031 19032 19033")
	do
		java -jar target/PBFT-1.0-SNAPSHOT-jar-with-dependencies.jar server 223.194.70.127 $i 2>&1 &
	done'
