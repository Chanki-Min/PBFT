package kr.ac.hongik.apl;

import java.io.Serializable;

public interface Operation extends Serializable {
    Result execute();
}
