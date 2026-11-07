package org.example.ab.attribution;

import java.io.Serializable;

/**
 * 某个用户在某个实验上最近一次曝光。Flink 把它存在 keyed state 里，
 * 用来判断后面的 click / purchase 能不能算进这次实验。
 */
public class ExposureState implements Serializable {

    public long timestamp;
    public String variantId;

    public ExposureState() {
    }

    public ExposureState(long timestamp, String variantId) {
        this.timestamp = timestamp;
        this.variantId = variantId;
    }
}
