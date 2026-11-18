package writable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义组合 Key，用于 Secondary Sort (二次排序)。
 * 包含股票代码和交易时间。
 */
public class StockTimeKey implements WritableComparable<StockTimeKey> {

    private String stockCode;
    private long tradeTime;

    /**
     * 必须提供一个无参构造函数。
     * Hadoop 在反序列化时会通过反射调用它来创建对象实例。
     */
    public StockTimeKey() {
    }

    /** 获取股票代码 */
    public String getStockCode() {
        return stockCode;
    }
    /** 获取交易时间 */
    public long getTradeTime() {
        return tradeTime;
    }


    /**
     * 序列化方法：将对象转换为字节流，以便在网络传输或写入磁盘。
     * 注意：写入顺序必须与 readFields 读取顺序完全一致。
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(stockCode); // 写入字符串
        out.writeLong(tradeTime); // 写入长整型
    }

    /**
     * 反序列化方法：从字节流中读取数据，还原成对象。
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.stockCode = in.readUTF();
        this.tradeTime = in.readLong();
    }

    /**
     * 比较方法：决定排序规则 (核心逻辑)。
     * 规则：
     * 1. 先按 stockCode 升序排列。
     * 2. 如果 stockCode 相同，则按 tradeTime 升序排列。
     */
    @Override
    public int compareTo(StockTimeKey o) {
        // 1. 比较股票代码
        int cmp = this.stockCode.compareTo(o.stockCode);
        if (cmp != 0) {
            return cmp;
        }
        // 2. 如果股票代码相同，比较时间
        return Long.compare(this.tradeTime, o.tradeTime);
    }

    /**
     * 重写 toString，方便调试和日志打印
     */
    @Override
    public String toString() {
        return stockCode + "\t" + tradeTime;
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(StockTimeKey.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                // 1. 解析 StockCode (String)
                int n1 = WritableUtils.decodeVIntSize(b1[s1]);
                int len1 = readVInt(b1, s1);
                int n2 = WritableUtils.decodeVIntSize(b2[s2]);
                int len2 = readVInt(b2, s2);

                // 比较 String 字节
                int cmp = compareBytes(b1, s1 + n1, len1, b2, s2 + n2, len2);
                if (cmp != 0) return cmp;

                // 2. 解析 TradeTime (Long)
                // String 之后就是 Long，位置是 start + VIntSize + StringLen
                long time1 = readLong(b1, s1 + n1 + len1);
                long time2 = readLong(b2, s2 + n2 + len2);

                return Long.compare(time1, time2);
            } catch (IOException e) {
                return super.compare(b1, s1, l1, b2, s2, l2);
            }
        }
    }
    /**
     * hashCode方法：
     * 分布式计算中Partitioner利用hashCode决定Key发往哪个Reducer。
     * 必须保证相同stockCode的记录拥有相同的hash值，从而被分配到同一个节点进行时序计算。
     */
    @Override
    public int hashCode() {
        return stockCode.hashCode() * 31 + (int) tradeTime;
    }

    /**
     * 确保在 Map 端去重或内存操作中能够准确识别相同的复合键。
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StockTimeKey) {
            StockTimeKey other = (StockTimeKey) obj;
            return this.stockCode.equals(other.stockCode) && this.tradeTime == other.tradeTime;
        }
        return false;
    }
}