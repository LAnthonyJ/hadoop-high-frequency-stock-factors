package writable;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Job 1 Mapper -> Reducer 的传输对象。
 * 仅包含计算 20 个因子所需的最小数据集 (前5档价量 + 总买卖量)。
 */
public class RawDataWritable implements Writable, Cloneable {

    // 使用数组存储前 5 档数据，方便循环计算
    private double[] askPrices = new double[5];   // 卖价 ap1-ap5
    private double[] bidPrices = new double[5];   // 买价 bp1-bp5
    private double[] askVolumes = new double[5];  // 卖量 av1-av5
    private double[] bidVolumes = new double[5];  // 买量 bv1-bv5

    private double totalAskVol; // 全市场卖单总量
    private double totalBidVol; // 全市场买单总量

    public RawDataWritable() {
    }

    /**
     * 极速赋值，直接引用Mapper的临时buffer(零拷贝)
     */
    public void set(double[] ap, double[] bp, double[] av, double[] bv, double tAsk, double tBid) {
        // 将Mapper解析出的临时buffer数据同步到本对象的内部数组中
        System.arraycopy(ap, 0, this.askPrices, 0, 5);
        System.arraycopy(bp, 0, this.bidPrices, 0, 5);
        System.arraycopy(av, 0, this.askVolumes, 0, 5);
        System.arraycopy(bv, 0, this.bidVolumes, 0, 5);
        this.totalAskVol = tAsk;
        this.totalBidVol = tBid;
    }
    /**
     * 将22个双精度浮点数转为二进制字节流。
     * 按照5档循环写入。
     * 这种纯二进制布局不含字段名，是 Hadoop 中最高效的序列化方式。
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // 依次写入5档数据
        for (int i = 0; i < 5; i++) {
            out.writeDouble(askPrices[i]);
            out.writeDouble(bidPrices[i]);
            out.writeDouble(askVolumes[i]);
            out.writeDouble(bidVolumes[i]);
        }
        // 写入全市场汇总总量
        out.writeDouble(totalAskVol);
        out.writeDouble(totalBidVol);
    }
    /**
     * 反序列化逻辑：从二进制流中还原5档价量数据。
     * 必须严格遵守write方法定义的写入顺序。
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // 必须按照 write 的顺序读取
        for (int i = 0; i < 5; i++) {
            askPrices[i] = in.readDouble();
            bidPrices[i] = in.readDouble();
            askVolumes[i] = in.readDouble();
            bidVolumes[i] = in.readDouble();
        }
        totalAskVol = in.readDouble();
        totalBidVol = in.readDouble();
    }

    // Getters，供FactorCalculator使用
    public double[] getAskPrices() { return askPrices; }
    public double[] getBidPrices() { return bidPrices; }
    public double[] getAskVolumes() { return askVolumes; }
    public double[] getBidVolumes() { return bidVolumes; }
    public double getTotalAskVol() { return totalAskVol; }
    public double getTotalBidVol() { return totalBidVol; }
    /**
     * 重写 toString 方法：
     * 展示最具代表性的ap1和bp1，方便在调试阶段监控数据流的正确性。
     */
    @Override
    public String toString() {
        return "RawData{" +
                "ap1=" + askPrices[0] +
                ", bp1=" + bidPrices[0] +
                '}';
    }


}