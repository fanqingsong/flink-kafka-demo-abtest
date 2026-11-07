package org.example.ab.attribution;

/**
 * 7 天归因窗口里的两条判定。窗口长度由调用方传入，这里不写死天数。
 */
public final class AttributionRules {

    private AttributionRules() {
    }

    /**
     * 窗口内的第一次曝光记为已归因。没有历史曝光，或者距离上次曝光已经超过窗口，都算第一次。
     * 窗口内的再次曝光仍会更新“最近曝光”，但自己的 attributed 是 0。
     */
    public static boolean isFirstExposure(ExposureState current, long eventTimestamp, long windowMillis) {
        return current == null || eventTimestamp - current.timestamp > windowMillis;
    }

    /**
     * click / purchase 要同时满足：有过曝光、版本和这次曝光相同、发生在曝光之后、间隔不超过窗口。
     */
    public static boolean isWithinAttribution(
            ExposureState current,
            String variantId,
            long eventTimestamp,
            long windowMillis) {
        return current != null
                && current.variantId.equals(variantId)
                && eventTimestamp >= current.timestamp
                && eventTimestamp - current.timestamp <= windowMillis;
    }
}
