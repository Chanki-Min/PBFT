package kr.ac.hongik.apl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

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
				default:
					throw new IllegalArgumentException();
			}
		} catch (IndexOutOfBoundsException | IllegalArgumentException e) {
			String name = new File(Launcher.class.getProtectionDomain()
					.getCodeSource()
					.getLocation()
					.getPath()
			).getName();
			System.err.println(e);
			System.err.printf("Usage: java -jar %s server|client [<ip> <port>]\n", name);
			System.err.printf("Usage: java -jar %s monitor [<time as seconds, default = 1Hour>]\n", name);
		}
	}
}
