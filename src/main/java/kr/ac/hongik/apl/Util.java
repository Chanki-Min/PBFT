package kr.ac.hongik.apl;

import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.*;
import java.util.Formatter;
import java.util.stream.IntStream;

public class Util {

    public static void fastCopy(final ReadableByteChannel src, final WritableByteChannel dest) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);
        while (src.read(buffer) > 0) {
            //buffer.flip();
            dest.write(buffer);
            buffer.compact();
            if(Replica.DEBUG)
                System.err.println("read " + buffer.toString());
        }
        buffer.flip();
        while (buffer.hasRemaining()) {
            dest.write(buffer);
        }
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
    public static byte[] sign(PrivateKey key, Serializable message) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, NoSuchProviderException {
        Signature signature = Signature.getInstance("SHA1withRSA");
        signature.initSign(key);
        signature.update(serialize(message));
        return signature.sign();
    }

    /**
     * @param key              A public key to check for the owner
     * @param message          Any object to verify the owner
     * @param digitalSignature
     * @return
     */
    public static boolean verify(PublicKey key, Serializable message, byte[] digitalSignature) {
        try {
            Signature signature = Signature.getInstance("SHA1withRSA");
            signature.initVerify(key);
            signature.update(serialize(message));
            return signature.verify(digitalSignature);
        } catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
            e.printStackTrace();
        }
        return false;
    }
}
