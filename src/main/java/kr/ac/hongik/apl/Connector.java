package kr.ac.hongik.apl;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class Connector {
    protected List<Endpoint> endpoints;
    protected List<Socket> sockets;
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

    static byte[] serialize(Message message){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(out);
            outputStream.writeObject(message);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return out.toByteArray();
    }

    static Message deserialize(byte[] bytes){
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        Message object = null;
        try {
            ObjectInputStream inputStream = new ObjectInputStream(in);
            object = (Message) inputStream.readObject();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return object;
    }

    /**
     * It is a exception free wrapper of send function.
     * It sends the data and also manage the wrong sockets
     * @param endpoint
     * @param message
     */
    protected void send(Endpoint endpoint, Message message){
        byte[] serializedMessage = serialize(message);
        sockets.stream()
                .filter(x -> x.getInetAddress().equals(endpoint.ip) && x.getPort() == endpoint.port)
                .forEach(x -> {
                    try {
                        OutputStream out = x.getOutputStream();
                        out.write(serializedMessage);
                        out.flush();
                    } catch (IOException | NullPointerException e) {
                        e.printStackTrace();
                        //Close previous connection
                        try {
                            x.close();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                        sockets.remove(x);
                        //Reconnect
                        sockets.add(makeConnectionOrNull(endpoint));
                    }
                });
    }

    private Socket makeConnectionOrNull(Endpoint endpoint){
        try {
            Socket socket = new Socket(endpoint.ip, endpoint.port);
            return socket;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void makeConnections() {
        sockets = new ArrayList<>();
        endpoints.stream().forEach(x -> sockets.add(makeConnectionOrNull(x)));
    }


}
