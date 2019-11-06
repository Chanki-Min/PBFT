package kr.ac.hongik.apl;

import com.owlike.genson.Genson;
import kr.ac.hongik.apl.Generator.Generator;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.GreetingOperation;
import kr.ac.hongik.apl.Operations.Operation;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Demo {
	public static void main(String[] args) throws IOException, NoSuchFieldException {
		InputStream in = Demo.class.getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		String initPath = "/GEN_initData/Init_data00.json";
		Generator generator = new Generator(initPath, true);

		Client client = new Client(prop);
		for(int i = 0; i < 10; ++i){
			String json = new Genson().serialize(generator.generate());
			Operation op = new GreetingOperation(client.getPublicKey(), json);
			RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
			client.request(requestMessage);
			var ret = client.getReply();

			//System.out.println(ret.toString());
		}
		client.printTurnAroundTime();

	}
}
