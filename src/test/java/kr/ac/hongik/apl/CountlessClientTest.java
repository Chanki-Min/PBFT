package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.net.*;
import java.nio.channels.SocketChannel;
import java.util.Properties;


public class CountlessClientTest extends Thread{
    Client client;
    Operation op;
    RequestMessage requestMessage;
    int clientNum;

    public CountlessClientTest(Properties prop, int clientNum){
        this.client = new Client(prop);
        this.op = new GreetingOperation(client.getPublicKey());
        this.requestMessage = new RequestMessage(client.getPrivateKey(), op);
        this.clientNum = clientNum;
    }

    @Override
    public void run(){
        System.err.printf("Client %d start", this.clientNum);
        this.client.request(this.requestMessage);
        var ret = this.client.getReply();
        System.err.printf("client %d end\n", this.clientNum);
        Assertions.assertEquals("Hello, World!", ret.toString());
    }
}
