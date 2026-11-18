package util;

import writable.RawDataWritable;

public class FactorCalculator {

    //定义极小值
    private static final double EPSILON = 1e-7;

    /**
     * 安全除法逻辑：解决量化计算中的分母为零难点。
     */
    private static double safeDivide(double num, double denom) {
        if (denom > -1e-9 && denom < 1e-9) {
            return num / (denom + EPSILON);
        }
        return num / denom;
    }

    /**
     * 核心计算方法：实现 LOB（订单簿）数据的因子转化。
     * @param data      当前时刻的 LOB 原始数据（买卖 5 档价量）
     * @param factors   【性能关键】复用的结果数组。由外部传入，避免内部申请堆内存。
     * @param prevAp1   上一时刻全市场平均卖一价（用于差分因子计算）
     * @param prevMid   上一时刻全市场平均中间价
     * @param prevRatio 上一时刻全市场平均深度比
     */
    public static void calculate(RawDataWritable data, double[] factors,
                                 double prevAp1, double prevMid, double prevRatio) {

        // 1. 提取原始数据的引用 (无拷贝开销)
        double[] ap = data.getAskPrices();
        double[] bp = data.getBidPrices();
        double[] av = data.getAskVolumes();
        double[] bv = data.getBidVolumes();

        // 2. 预计算与中间变量初始化
        double sumAskVol = 0.0;
        double sumBidVol = 0.0;
        double wAskPriceSum = 0.0; // 卖方价格加权分子
        double wBidPriceSum = 0.0; // 买方价格加权分子
        double decayAskVol = 0.0;  // 卖方衰减深度
        double decayBidVol = 0.0;  // 买方衰减深度

        // 3. 单次循环计算所有累加值
        for (int i = 0; i < 5; i++) {
            double aV = av[i];
            double bV = bv[i];
            double aP = ap[i];
            double bP = bp[i];
            // 基础深度累加
            sumAskVol += aV;
            sumBidVol += bV;

            wAskPriceSum += aP * aV;
            wBidPriceSum += bP * bV;

            double weight = i + 1.0;
            decayAskVol += aV / weight;
            decayBidVol += bV / weight;
        }

        // 4. 计算并填充无状态因子 (F1-F16, F20)
        double bp1 = bp[0];
        double ap1 = ap[0];

        double f1_spread = ap1 - bp1;
        double f3_mid = (ap1 + bp1) * 0.5;
        double f9_ratio = safeDivide(sumBidVol, sumAskVol);

        factors[0] = f1_spread;                                     // F1: 最优价差
        factors[1] = safeDivide(f1_spread, f3_mid);                 // F2: 相对价差
        factors[2] = f3_mid;                                        // F3: 中间价
        factors[3] = safeDivide(bv[0] - av[0], bv[0] + av[0]);      // F4: 买一不平衡
        factors[4] = safeDivide(sumBidVol - sumAskVol, sumBidVol + sumAskVol); // F5: 多档不平衡
        factors[5] = sumBidVol;                                     // F6: 买方深度
        factors[6] = sumAskVol;                                     // F7: 卖方深度
        factors[7] = sumBidVol - sumAskVol;                         // F8: 深度差
        factors[8] = f9_ratio;                                      // F9: 深度比

        double tB = data.getTotalBidVol();
        double tA = data.getTotalAskVol();
        factors[9] = safeDivide(tB - tA, tB + tA);      // F10: 买卖量平衡

        double vwapBid = safeDivide(wBidPriceSum, sumBidVol);
        double vwapAsk = safeDivide(wAskPriceSum, sumAskVol);
        factors[10] = vwapBid;                                      // F11: VWAP Bid
        factors[11] = vwapAsk;                                      // F12: VWAP Ask
        factors[12] = safeDivide(wBidPriceSum + wAskPriceSum, sumBidVol + sumAskVol); // F13: 加权中间价
        factors[13] = vwapAsk - vwapBid;                            // F14: 加权价差

        // F15: 买卖密度差 (avg(bv) - avg(av)) = (sum/5 - sum/5)
        factors[14] = (sumBidVol * 0.2) - (sumAskVol * 0.2);

        // F16: 买卖不对称度
        factors[15] = safeDivide(decayBidVol - decayAskVol, decayBidVol + decayAskVol);

        // F20: 价压指标
        factors[19] = safeDivide(f1_spread, sumBidVol + sumAskVol);

        // 5. 计算时序因子 (F17-19)
        // 检查 prevAp1 是否为 NaN (标志着这是第一条数据)
        if (Double.isNaN(prevAp1)) {
            factors[16] = 0.0;
            factors[17] = 0.0;
            factors[18] = 0.0;
        } else {
            factors[16] = ap1 - prevAp1;         // F17: 最优价变动
            factors[17] = f3_mid - prevMid;      // F18: 中间价变动
            factors[18] = f9_ratio - prevRatio;  // F19: 深度比变动
        }
    }
}