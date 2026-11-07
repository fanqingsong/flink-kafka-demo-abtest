package org.example.ab;

import org.example.ab.attribution.AttributionRules;
import org.example.ab.attribution.ExposureState;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventAnalyticsJobTest {

    private static final long WINDOW = Duration.ofDays(7).toMillis();

    @Test
    void attributesOnlyMatchingVariantInsideWindow() {
        ExposureState exposure = new ExposureState(1_000L, "variant-a");

        assertTrue(AttributionRules.isWithinAttribution(exposure, "variant-a", 2_000L, WINDOW));
        assertFalse(AttributionRules.isWithinAttribution(exposure, "variant-b", 2_000L, WINDOW));
        assertFalse(AttributionRules.isWithinAttribution(exposure, "variant-a", 999L, WINDOW));
        assertFalse(AttributionRules.isWithinAttribution(
                exposure, "variant-a", 1_000L + WINDOW + 1, WINDOW));
    }

    @Test
    void marksOnlyFirstExposureWithinAttributionWindow() {
        assertTrue(AttributionRules.isFirstExposure(null, 1_000L, WINDOW));
        ExposureState exposure = new ExposureState(1_000L, "variant-a");
        assertFalse(AttributionRules.isFirstExposure(exposure, 2_000L, WINDOW));
        assertTrue(AttributionRules.isFirstExposure(
                exposure, 1_000L + WINDOW + 1, WINDOW));
    }
}
