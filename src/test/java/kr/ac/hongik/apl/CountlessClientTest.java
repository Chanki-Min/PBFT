package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;

import java.time.Instant;
import java.util.Properties;


public class CountlessClientTest extends Thread{
    Client client;
    Operation op;
    RequestMessage requestMessage;
    int clientNum;

    public CountlessClientTest(Properties prop, int clientNum){
        this.client = new Client(prop);
        this.op = new GreetingOperation(client.getPublicKey());
		this.requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
        this.clientNum = clientNum;
    }

    @Override
    public void run(){
        var beg = Instant.now().toEpochMilli();
        System.err.printf("Client %d start\n", this.clientNum);
        this.client.request(this.requestMessage);
        var ret = this.client.getReply();
        var end = Instant.now().toEpochMilli();
        System.err.printf("client %d end, %d milli seconds\n", this.clientNum, (int) (end - beg));
        Assertions.assertEquals("Hello, World!", ret.toString());
    }
}
