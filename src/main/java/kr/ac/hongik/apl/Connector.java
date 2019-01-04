package kr.ac.hongik.apl;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Connector {
    List<Endpoint> endpoints;
    List<Socket> sockets;
    int numOfReplica;

    public Connector() { }

    public Connector(Properties prop) {
        numOfReplica = Integer.parseInt( prop.getProperty("replica"));

        endpoints = new ArrayList<>();
        for (int i = 0; i < numOfReplica; i++) {
            String addr = prop.getProperty("replica" + i);
            String[] parsedAddr = addr.split(":");

            Endpoint endpoint = new Endpoint(parsedAddr[0], Integer.parseInt(parsedAddr[1]));
            System.out.print("ip : " + endpoint.ip);
            System.out.println("\t" + "port : " + endpoint.port);
            endpoints.add(endpoint);
        }

        makeConnections();
    }

    private void makeConnections() {
        sockets = new ArrayList<>(endpoints.size());

        for (int i = 0; i < endpoints.size(); i++) {
            Endpoint endpoint = endpoints.get(i);
            try {
                sockets.set(i, new Socket(endpoint.ip, endpoint.port));
            } catch (IOException e) {
                e.printStackTrace();
                sockets.set(i, null);
            }
        }
    }


}
