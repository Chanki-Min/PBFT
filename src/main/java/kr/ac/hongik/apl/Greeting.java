package kr.ac.hongik.apl;

public class Greeting implements Result {
    public String greeting;

    public Greeting(String greeting) {
        this.greeting = greeting;
    }

    @Override
    public String toString() {
        return this.greeting;
    }
}