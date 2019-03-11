package kr.ac.hongik.apl;

import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/* TODO 구매 시나리오를 따라서 구현할 예정 */
public class Management extends Operation {
    private final List<InetSocketAddress> replicaAddresses;
    private final BlockPayload blockPayload;

    protected Management(PublicKey clientInfo, Properties replicasInfo, BlockPayload blockPayload) {
        super(clientInfo, Instant.now().getEpochSecond());
        replicaAddresses = Util.parseProperties(replicasInfo);
        this.blockPayload = blockPayload;
    }

    @Override
    Result execute() {
        //1. Create a block (by using SQL)
        //2. Split the certs and the replica store each pieces.

        //3. Also, We have to reconstruct and verify the certs

        return null;
    }
}
