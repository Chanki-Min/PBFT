package kr.ac.hongik.apl;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.security.*;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

public class Util {
    static final String ALGORITHM = "SHA1withRSA";


    public static List<InetSocketAddress> parseProperties(Properties prop) {
        List<InetSocketAddress> replicaAddresses = new ArrayList<>();
        var numOfReplica = Integer.parseInt(prop.getProperty("replica"));

        for (int i = 0; i < numOfReplica; i++) {
            String addressInString = prop.getProperty("replica" + i);
            String[] parsedAddress = addressInString.split(":");

            InetSocketAddress address = new InetSocketAddress(parsedAddress[0], Integer.parseInt(parsedAddress[1]));
            replicaAddresses.add(address);
        }
        return replicaAddresses;
    }

    public static String hash(Serializable obj) {
        return hash(serialize(obj));
    }

    public static String hash(final byte[] bytes) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return bytesToHex(digest.digest(bytes));
    }

    public static String bytesToHex(final byte[] bytes) {
        return IntStream.range(0, bytes.length)
                .collect(StringBuilder::new,
                        (sb, i) -> new Formatter(sb).format("%02x", bytes[i] & 0xFF),
                        StringBuilder::append).toString();
    }

    public static byte[] serialize(Serializable message) {
        if (message instanceof String)
            return ((String) message).getBytes();
        else
            return SerializationUtils.serialize(message);
    }

    public static Serializable deserialize(byte[] bytes) {
        return SerializationUtils.deserialize(bytes);
    }

    /**
     * Return (private, public) key pair.
     * To get each keys, use KeyPair.getPublic and KeyPair.getPrivate methods.
     *
     * @return KeyPair object
     * @throws NoSuchAlgorithmException
     */
    public static KeyPair generateKeyPair() {
        KeyPairGenerator generator = null;
        try {
            generator = KeyPairGenerator.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return generator.generateKeyPair();
    }

    /**
     * @param key     A private key to identify itself.
     * @param message Any object to identify the owner.
     * @return A byte array which contains a digital signature
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws SignatureException
     * @throws NoSuchProviderException
     */
    public static byte[] sign(PrivateKey key, Serializable message)  {
        try {
            Signature signature = Signature.getInstance(ALGORITHM);
            signature.initSign(key);
            signature.update(serialize(message));
            return signature.sign();
        } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param key              A public key to check for the owner
     * @param message          Any object to verify the owner
     * @param digitalSignature
     * @return
     */
    public static boolean verify(PublicKey key, Serializable message, byte[] digitalSignature) {
        try {
            Signature signature = Signature.getInstance(ALGORITHM);
            signature.initVerify(key);
            signature.update(serialize(message));
            return signature.verify(digitalSignature);
        } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            e.printStackTrace();
            return false;
        }
    }
}
