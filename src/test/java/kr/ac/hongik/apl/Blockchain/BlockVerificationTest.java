package kr.ac.hongik.apl.Blockchain;

import kr.ac.hongik.apl.BlockVerificationThread;
import kr.ac.hongik.apl.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Scanner;

public class BlockVerificationTest {
	@Test
	public void verificationTest() throws InterruptedException, IOException{
		String filePath = "Please Insert Proper sqlDB Path that stores HEADER";
		String esIndexName = "block_chain";
		String dbTableName = "BlockChain";
		boolean trigger = true;
		Logger logger = new Logger(filePath);

		Scanner scn = new Scanner(System.in);
		Thread verifyThread = new BlockVerificationThread(logger, esIndexName, dbTableName);
		verifyThread.start();
		verifyThread.join();
	}
}
