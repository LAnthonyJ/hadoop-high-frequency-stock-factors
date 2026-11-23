package writable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Composite key for secondary sorting.
 * Combines stock code and trade time.
 */
public class StockTimeKey implements WritableComparable<StockTimeKey> {

    private String stockCode;
    private long tradeTime;

    /**
     * Provide a no-argument constructor.
     * Hadoop instantiates keys through reflection during deserialization.
     */
    public StockTimeKey() {
    }

    /** Return the stock code. */
    public String getStockCode() {
        return stockCode;
    }
    /** Return the trade time. */
    public long getTradeTime() {
        return tradeTime;
    }


    /**
     * Serialize the key for storage or transfer.
     * Use the same field order as readFields().
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(stockCode); // Write the stock code.
        out.writeLong(tradeTime); // Write the timestamp.
    }

    /**
     * Read the key from its binary representation.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.stockCode = in.readUTF();
        this.tradeTime = in.readLong();
    }

    /**
     * Compare the two components of the key.
     * Sort order:
     * First compare stock codes in ascending order.
     * For identical codes, sort by trade time.
     */
    @Override
    public int compareTo(StockTimeKey o) {
        // Compare stock codes.
        int cmp = this.stockCode.compareTo(o.stockCode);
        if (cmp != 0) {
            return cmp;
        }
        // Break ties using the trade time.
        return Long.compare(this.tradeTime, o.tradeTime);
    }

    /**
     * Format the key for diagnostic output.
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
                // Read the stock code string.
                int n1 = WritableUtils.decodeVIntSize(b1[s1]);
                int len1 = readVInt(b1, s1);
                int n2 = WritableUtils.decodeVIntSize(b2[s2]);
                int len2 = readVInt(b2, s2);

                // Compare the serialized string bytes.
                int cmp = compareBytes(b1, s1 + n1, len1, b2, s2 + n2, len2);
                if (cmp != 0) return cmp;

                // Read the long timestamp.
                // The timestamp follows the variable-length size prefix and stock code bytes.
                long time1 = readLong(b1, s1 + n1 + len1);
                long time2 = readLong(b2, s2 + n2 + len2);

                return Long.compare(time1, time2);
            } catch (IOException e) {
                return super.compare(b1, s1, l1, b2, s2, l2);
            }
        }
    }
    /**
     * Compute a hash for partitioning.
     * The partitioner uses the hash to choose a reducer.
     * Use a consistent hash for each stock so its time series stays on one reducer.
     */
    @Override
    public int hashCode() {
        return stockCode.hashCode() * 31 + (int) tradeTime;
    }

    /**
     * Use both fields when checking equality in memory.
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