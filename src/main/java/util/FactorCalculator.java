package util;

import writable.RawDataWritable;

public class FactorCalculator {

    // Tolerance for near-zero denominators.
    private static final double EPSILON = 1e-7;

    /**
     * Handle division by zero in the factor calculations.
     */
    private static double safeDivide(double num, double denom) {
        if (denom > -1e-9 && denom < 1e-9) {
            return num / (denom + EPSILON);
        }
        return num / denom;
    }

    /**
     * Convert limit order book snapshots into factors.
     * @param data prices and volumes at the first five bid and ask levels
     * @param factors caller-owned output array reused across snapshots
     * @param prevAp1 previous market-average best ask price
     * @param prevMid previous market-average midpoint
     * @param prevRatio previous market-average depth ratio
     */
    public static void calculate(RawDataWritable data, double[] factors,
                                 double prevAp1, double prevMid, double prevRatio) {

        // Read the underlying arrays without copying.
        double[] ap = data.getAskPrices();
        double[] bp = data.getBidPrices();
        double[] av = data.getAskVolumes();
        double[] bv = data.getBidVolumes();

        // Initialize intermediate values.
        double sumAskVol = 0.0;
        double sumBidVol = 0.0;
        double wAskPriceSum = 0.0; // Weighted ask price numerator
        double wBidPriceSum = 0.0; // Weighted bid price numerator
        double decayAskVol = 0.0;  // Decayed ask depth
        double decayBidVol = 0.0;  // Decayed bid depth

        // Accumulate all levels in a single pass.
        for (int i = 0; i < 5; i++) {
            double aV = av[i];
            double bV = bv[i];
            double aP = ap[i];
            double bP = bp[i];
            // Accumulate order book depth.
            sumAskVol += aV;
            sumBidVol += bV;

            wAskPriceSum += aP * aV;
            wBidPriceSum += bP * bV;

            double weight = i + 1.0;
            decayAskVol += aV / weight;
            decayBidVol += bV / weight;
        }

        // Compute stateless factors F1–F16 and F20.
        double bp1 = bp[0];
        double ap1 = ap[0];

        double f1_spread = ap1 - bp1;
        double f3_mid = (ap1 + bp1) * 0.5;
        double f9_ratio = safeDivide(sumBidVol, sumAskVol);

        factors[0] = f1_spread;                                     // F1: best bid–ask spread
        factors[1] = safeDivide(f1_spread, f3_mid);                 // F2: relative spread
        factors[2] = f3_mid;                                        // F3: midpoint
        factors[3] = safeDivide(bv[0] - av[0], bv[0] + av[0]);      // F4: top-of-book imbalance
        factors[4] = safeDivide(sumBidVol - sumAskVol, sumBidVol + sumAskVol); // F5: multi-level imbalance
        factors[5] = sumBidVol;                                     // F6: bid depth
        factors[6] = sumAskVol;                                     // F7: ask depth
        factors[7] = sumBidVol - sumAskVol;                         // F8: depth difference
        factors[8] = f9_ratio;                                      // F9: depth ratio

        double tB = data.getTotalBidVol();
        double tA = data.getTotalAskVol();
        factors[9] = safeDivide(tB - tA, tB + tA);      // F10: bid–ask volume balance

        double vwapBid = safeDivide(wBidPriceSum, sumBidVol);
        double vwapAsk = safeDivide(wAskPriceSum, sumAskVol);
        factors[10] = vwapBid;                                      // F11: VWAP Bid
        factors[11] = vwapAsk;                                      // F12: VWAP Ask
        factors[12] = safeDivide(wBidPriceSum + wAskPriceSum, sumBidVol + sumAskVol); // F13: weighted midpoint
        factors[13] = vwapAsk - vwapBid;                            // F14: weighted spread

        // F15: difference between average bid and ask volume across five levels
        factors[14] = (sumBidVol * 0.2) - (sumAskVol * 0.2);

        // F16: bid–ask asymmetry
        factors[15] = safeDivide(decayBidVol - decayAskVol, decayBidVol + decayAskVol);

        // F20: price pressure
        factors[19] = safeDivide(f1_spread, sumBidVol + sumAskVol);

        // Compute time-dependent factors F17–F19.
        // A missing previous best ask marks the first snapshot.
        if (Double.isNaN(prevAp1)) {
            factors[16] = 0.0;
            factors[17] = 0.0;
            factors[18] = 0.0;
        } else {
            factors[16] = ap1 - prevAp1;         // F17: change in best ask
            factors[17] = f3_mid - prevMid;      // F18: change in midpoint
            factors[18] = f9_ratio - prevRatio;  // F19: change in depth ratio
        }
    }
}