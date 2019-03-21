package kr.ac.hongik.apl;

import com.codahale.shamir.Scheme;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static kr.ac.hongik.apl.RequestMessage.makeRequestMsg;

/**
 * 작품 등록, 인증서 발급 및 인증서의 유효성 검사를 판단하는 static methods의 집합이다.
 */
public class Function {

    /**
     * 작품을 거래하거나 작품을 등록하고 그 결과로 작품 거래에 대한 인증서를 반환한다.
     * 작품의 인증서를 발급받기위해서는 거래기록이 있어야 한다.
     * 작품을 등록할 경우, seller와 buyer를 동일하게 설정하고, price를 0으로 설정한다.
     * 작품 거래의 경우에는 각 매개변수의 원래 목적에 맞게 사용한다.
     *
     *
     * @param artName 작품번호, 작품 해시 등 각 작품을 유일하게 나타낼 수 있는 문자열이어야 한다.
     * @param seller 판매자를 유일하게 구분할 수 있어야 한다.
     * @param buyer 구매자를 유일하게 식별할 수 있어야 한다.
     * @param price 음수값은 불허한다.
     * @return  2개의 인증서를 포함하는 String[]를 반환한다. 첫번째 원소는 구매자의 인증서이고 두번째 원소는 판매자의 인증서이다.
     * @throws IOException jar파일 내부에 replica.properties가 없을 경우 발생한다.
     */
    public static String[] enroll(String artName, String seller, String buyer, int price) throws IOException {

        if(price < 0)
            throw new RuntimeException("Price >= 0");

        /****** Create some blocks to test ******/
        InputStream in = Function.class.getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);
        var replicas = Util.parseProperties(prop);

        Client client = new Client(prop);
        BlockCreation blockCreation = new BlockCreation(client.getPublicKey(), artName, seller, buyer, price, -1L);
        client.request(makeRequestMsg(client.getPrivateKey(), blockCreation));
        String header = (String) client.getReply();

        int n = replicas.size();
        int f = replicas.size() / 3;
        Map<Integer, byte[]> pieces = Util.split(header, n + 2, n - f + 1);

        client = new Client(prop);
        CertStorage certStorage = new CertStorage(client.getPublicKey(), pieces);
        client.request(makeRequestMsg(client.getPrivateKey(), certStorage));
        List<Object> roots = (List<Object>) client.getReply();
        String root = (String) roots.get(0);
        String buyerPiece = merge(5, pieces.get(5), root);
        String sellerPiece = merge(6, pieces.get(6), root);

        return new String[]{buyerPiece, sellerPiece};
    }

    /**
     * 발급된 인증서가 해당 작품에 대한 유효한 인증서인지 판단한다.
     *
     *
     * @param certificate 구매자 또는 판매자의 인증서를 받는다.
     * @param artName 작품의 식별자이다. 이 이름은 enroll에서 쓰인 것과 같은 이름이어야 한다.
     * @return 유효성이 검증되면 true, 실패했다면 false가 반환된다.
     * @throws IOException jar파일 내부에 replica.properties가 없을 경우 발생한다.
     */
    public static boolean validate(String certificate, String artName) throws IOException {
        InputStream in = Function.class.getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);
        var replicas = Util.parseProperties(prop);

        int buyerPieceNumber = Integer.valueOf(certificate.split(" ")[0]);
        byte[] buyerCertPiece = Base64.getDecoder().decode(certificate.split(" ")[1]);
        String root = certificate.split(" ")[2];

        Client client = new Client(prop);
        Collector collector = new Collector(client.getPublicKey(), root);
        client.request(makeRequestMsg(client.getPrivateKey(), collector));
        List<Object> prePieces = (List<Object>) client.getReply();

        //toMap 이용해서 키 조합
        Map<Integer, byte[]> pieces = Util.toMap(prePieces, buyerPieceNumber, buyerCertPiece);

        int n = replicas.size();
        int f = replicas.size() / 3;
        Scheme newScheme = new Scheme(new SecureRandom(), n + 2, n + 1 - f);
        String newHeader = new String(newScheme.join(pieces));

        //validation
        client = new Client(prop);
        Validation validation = new Validation(client.getPublicKey(), newHeader, artName);
        client.request(makeRequestMsg(client.getPrivateKey(), validation));

        return (Boolean) client.getReply();
    }

    private static String merge(int i, byte[] bytes, String root) {
        return String.format("%d %s %s", i, Base64.getEncoder().encodeToString(bytes), root);
    }

}
