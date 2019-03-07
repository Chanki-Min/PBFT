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
        System.err.println("Client" + this.clientNum + " start ");
        this.client.request(this.requestMessage);
        var ret = this.client.getReply();
        System.err.println("client" + this.clientNum + " end");
        Assertions.assertEquals("Hello, World!", ret.toString());
    }
}
