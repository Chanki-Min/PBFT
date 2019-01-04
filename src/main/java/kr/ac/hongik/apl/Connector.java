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
            String address = prop.getProperty("replica" + i);
            String[] parsedAddress = address.split(":");

            Endpoint endpoint = new Endpoint(parsedAddress[0], Integer.parseInt(parsedAddress[1]));
            endpoints.add(endpoint);
        }

        makeConnections();
    }

    void send(Endpoint endpoint, Message message){

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
