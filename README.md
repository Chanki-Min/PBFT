# Practical Byzantine Fault Tolerance

![description](./description.jpg)



PBFT는 네트워크 오류 및 신뢰할 수 없는 서버로부터 전체 서버의 Liveness와 Safety를 보장하는 분산 합의 알고리즘이다. 
Liveness는 클라이언트의 요청을 처리해 줄 수 있음을 보장하는 것 이고, Safety는 해당 시스템이 정상적으로 합의과정을 이행함을 의미한다.

## 목표
1. 수신받은 메시지를 저장할 Logger 클래스를 구현한다.
2. 정상 작동을 가정한 ***Normal-Case Operation***을 구현한다.
3. Primary의 비정상 작동을 고려한 ***View Change***단계를 구현한다.
4. Log를 특정 stable checkpoint마다 정리할 수 있도록 ***Garbage Collection***을 구현한다.

## TODO
* 메시지를 저장할 logger 클래스 구현, 혹은 적절한 클래스 import.
* Client 클래스 구현 - 김민균
* Replica 클래스 구현 - 문준오

## Reference
1. Practical Byzantine Fault Tolerance - <http://www.read.seas.harvard.edu/~kohler/class/cs239-w08/castro99practical.pdf>
