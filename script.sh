pkill java
mvn package
if [ $? -ne "0" ]
then
	exit
fi
for i in $(echo "19030 19031 19032")
do
   java -jar target/PBFT-0.5-SNAPSHOT-jar-with-dependencies.jar server 223.194.70.127 $i &
done
java -jar target/PBFT-0.5-SNAPSHOT-jar-with-dependencies.jar server 223.194.70.127 19033 &
