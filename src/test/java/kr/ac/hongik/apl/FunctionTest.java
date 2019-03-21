package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static kr.ac.hongik.apl.Function.enroll;
import static kr.ac.hongik.apl.Function.validate;

class FunctionTest {

    @Test
    void test() throws IOException {
        String certs = enroll("apl", "yoon", "Kim", 300)[0];
        Assertions.assertTrue(validate(certs, "apl"));
    }

    @Test
    void nameFaultTest() throws IOException {
        String certs = enroll("apl", "yoon", "Kim", 300)[0];
        Assertions.assertFalse(validate(certs, "song"));
    }
}