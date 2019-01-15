package kr.ac.hongik.apl;


import java.net.InetSocketAddress;
import java.security.*;

import static kr.ac.hongik.apl.Util.sign;

public class RequestMessage implements Message {

    private Operation operation;
    private byte[] signature;

    public RequestMessage(PrivateKey privateKey, Operation operation) {

        this.operation = operation;
        try {
            this.signature = sign(privateKey, this.operation);
        } catch (NoSuchProviderException | NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            e.printStackTrace();
        }
    }

    public Operation getOperation() {
        return this.operation;
    }

    public long getTime() {
        return this.operation.getTimestamp();
    }

    public InetSocketAddress getClientInfo() {
        return this.operation.getClientInfo();
    }

    public byte[] getSignature() {
        return this.signature;
    }
}
