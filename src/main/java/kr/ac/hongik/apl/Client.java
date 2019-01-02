package kr.ac.hongik.apl;

public interface Client {
    void request(Message msg);
    Result reply();
}
