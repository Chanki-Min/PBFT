package kr.ac.hongik.apl;

import java.io.*;
import java.security.*;
import java.util.Formatter;
import java.util.stream.IntStream;

public class Util {
    public static String hash(Message obj) {
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
     public static byte[] serialize(Message message){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(out);
            outputStream.writeObject(message);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return out.toByteArray();
    }

    public static Message deserialize(byte[] bytes){
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
     * Return (private, public) key pair.
     * To get each keys, use KeyPair.getPublic and KeyPair.getPrivate methods.
     * @return KeyPair object
     * @throws NoSuchAlgorithmException
     */
    public static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        return generator.generateKeyPair();
    }

    public static byte[] sign(PrivateKey key, Message message) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, NoSuchProviderException {
        Signature signature = Signature.getInstance("SHA1withRSA");
        signature.initSign(key);
        signature.update(serialize(message));
        return signature.sign();
    }
    public static boolean verify(PublicKey key, Message message, byte[] signatureBytes) {
        try {
            Signature signature = Signature.getInstance("SHA1withRSA");
            signature.initVerify(key);
            signature.update(serialize(message));
            return signature.verify(signatureBytes);
        } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            e.printStackTrace();
        }
        return false;
    }
}
