package kr.ac.hongik.apl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Launcher {
	public static void main(String[] args) throws IOException, NoSuchFieldException {
		try {
			switch (args[0].toLowerCase()) {
				case "server":
					Replica.main(Arrays.stream(args).skip(1).toArray(String[]::new));
					break;
				case "client":
					Client.main(Arrays.stream(args).skip(1).toArray(String[]::new));
					break;
				case "monitor":
					Monitor.main(Arrays.stream(args).skip(1).toArray(String[]::new));
					break;
				case "broker":
					Broker.main(Arrays.stream(args).skip(1).toArray(String[]::new));
					break;
				case "demo":
					Demo.main(Arrays.stream(args).skip(1).toArray(String[]::new));
					break;
				default:
					throw new IllegalArgumentException();
			}
		} catch (IndexOutOfBoundsException | IllegalArgumentException e) {
			String name = new File(Launcher.class.getProtectionDomain()
					.getCodeSource()
					.getLocation()
					.getPath()
			).getName();
			Replica.msgDebugger.error(e);
			Replica.msgDebugger.error("Usage: java -jar %s server|client [<publicIp> <publicPort> <virtualPort>]", name);
			Replica.msgDebugger.error("Usage: java -jar %s monitor [<time as seconds, default = 1Hour>]", name);
			Replica.msgDebugger.error("Usage: java -jar %s broker [<insert duration> <max queue size> <is data loop true|false>]", name);
			Replica.msgDebugger.error("Usage: java -jar %s Demo", name);
		}
	}
}
