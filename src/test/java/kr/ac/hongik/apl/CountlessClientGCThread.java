package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.GreetingOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;


public class CountlessClientGCThread extends Thread {
    private Client client;
    private Operation op;
    private RequestMessage requestMessage;
    private int clientNum;
    private int repeatTime;

    public CountlessClientGCThread(Properties prop, int clientNum, int repeatTime) {
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
            this.op = new GreetingOperation(client.getPublicKey());
            this.requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
            //var end = Instant.now().toEpochMilli();
            //System.err.printf("client %d end, %d milli seconds\n", this.clientNum, (int) (end - beg));
            Assertions.assertEquals("Hello, World!", ret.toString());
        }
    }
}
