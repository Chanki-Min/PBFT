package kr.ac.hongik.apl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * JAR 파일 실행시 main class 로 바인딩되어 args에 따라서 각 클래스의 main 메소드를 호출한다
 */
public class Launcher {
	public static void main(String[] args) throws IOException {
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
			Replica.msgDebugger.error(String.format("Usage: java -jar %s server|client [<publicIp> <publicPort> <virtualPort>]", name));
			Replica.msgDebugger.error(String.format("Usage: java -jar %s monitor [<time as seconds, default = 1Hour>]", name));
			Replica.msgDebugger.error(String.format("Usage: java -jar %s broker [<insert duration> <max queue size> <is data loop true|false>]", name));
		}
	}
}
