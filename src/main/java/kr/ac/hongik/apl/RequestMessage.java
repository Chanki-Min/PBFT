package kr.ac.hongik.apl;


import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;

public class RequestMessage implements Message {

    private Data data;
    private byte[] signature;

    public RequestMessage(PrivateKey privateKey, Operation operation, String ip, Integer port){

        this.data = new Data(operation, ip, port);
        this.signature = sign(privateKey, this.data);

    }

    public Operation getOperation(){

        return this.data.operation;
    }

    public String getTime(){

        return this.data.time;
    }

    public InetSocketAddress getClientInfo(){

        return this.data.clientInfo;
    }

    private class Data {
        private Operation operation;
        private String time;
        private InetSocketAddress clientInfo;

        public Data(Operation operation, String ip, Integer port){
            this.operation = operation;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.time = format.format(System.currentTimeMillis());
            this.clientInfo = new InetSocketAddress(ip, port);
        }
    }

}
