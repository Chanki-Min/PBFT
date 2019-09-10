package kr.ac.hongik.apl.Generator;

import org.junit.jupiter.api.Test;

public class GeneratorTest {
	@Test
	public void generatorTest() throws NoSuchFieldException {
		String initPath = "/GEN_initData/Init_data00.json";
		Generator generator = new Generator(initPath, true);

		while (true) {
			generator.generate();
		}
	}
}
