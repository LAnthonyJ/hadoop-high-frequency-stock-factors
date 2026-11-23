package job1;

import writable.StockTimeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Group intermediate keys by stock.
 * This comparator determines which values arrive in one reduce call.
 * Compare stockCode and ignore tradeTime.
 *
 * All timestamps for a stock then reach the same reduce call.
 * Secondary sorting orders the values by trade time.
 */
public class StockGroupingComparator extends WritableComparator {

    /**
     * Call the parent constructor explicitly.
     * Allow Hadoop to instantiate the StockTimeKey objects being compared.
     */
    public StockGroupingComparator() {
        super(StockTimeKey.class, true);
    }

    /**
     * Compare grouping keys.
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StockTimeKey k1 = (StockTimeKey) a;
        StockTimeKey k2 = (StockTimeKey) b;

        // Grouping uses the stock code alone.
        return k1.getStockCode().compareTo(k2.getStockCode());
    }
}