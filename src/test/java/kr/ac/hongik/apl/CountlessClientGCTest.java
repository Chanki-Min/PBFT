package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;

import java.util.Properties;


public class CountlessClientGCTest extends Thread {
    Client client;
    Operation op;
    RequestMessage requestMessage;
    int clientNum;
    int repeatTime;

    public CountlessClientGCTest(Properties prop, int clientNum, int repeatTime) {
        this.client = new Client(prop);
        this.op = new GreetingOperation(client.getPublicKey());
        this.requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
        this.clientNum = clientNum;
        this.repeatTime = repeatTime;
    }

    @Override
    public void run() {
        //var beg = Instant.now().toEpochMilli();
        for (int i = 1; i <= this.repeatTime; i++) {
            System.err.printf("Client %d request #%dstart\n", this.clientNum, i);
            this.client.request(this.requestMessage);
            var ret = this.client.getReply();
            //var end = Instant.now().toEpochMilli();
            //System.err.printf("client %d end, %d milli seconds\n", this.clientNum, (int) (end - beg));
            Assertions.assertEquals("Hello, World!", ret.toString());
        }
    }
}
