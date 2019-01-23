package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;


public class RunnableTest {
    @Test
    void test() throws IOException {
        try {
            InputStream in = new FileInputStream("src/test/resources/replica.properties");
            Properties prop = new Properties();
            prop.load(in);
            List<Replica> replicas = IntStream.range(0, 4).mapToObj(i -> {
                String addr = prop.getProperty("replica" + i);
                var addrs = addr.split(":");
                var ret = new Replica(prop, addrs[0], Integer.parseInt(addrs[1]));
                return ret;
            }).collect(toList());

            replicas.parallelStream().forEach(Connector::connect);
        } finally {
            File file = new File("src/main/resources/");
            Arrays.stream(file.listFiles())
                    .filter(x -> x.getName().contains(".db"))
                    .forEach(File::delete);
        }
    }
}
