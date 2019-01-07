package kr.ac.hongik.apl;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Endpoint {
    public final InetAddress ip;
    public final int port;

    public Endpoint(String ip, int port){
        InetAddress tmpIp;
        try {
            tmpIp = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            tmpIp = null;
        }
        this.ip = tmpIp;
        this.port = port;
    }
}
