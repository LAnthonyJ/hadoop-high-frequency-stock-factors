package job1;

import writable.StockTimeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器 (Grouping Comparator)
 * 作用：决定 Reducer 的 reduce 方法中 values 迭代器包含哪些数据。
 * 逻辑：只比较股票代码 (stockCode)，忽略时间 (tradeTime)。
 *
 * 结果：同一只股票的所有时间点的数据，会被汇总到一次 reduce 调用中。
 * 配合之前的排序（Sort），这些数据在迭代器中是严格按时间有序的。
 */
public class StockGroupingComparator extends WritableComparator {

    /**
     * 必须显式调用父类构造函数。
     * 告诉 Hadoop 我们要比较的对象类型是 StockTimeKey，并允许它创建实例。
     */
    public StockGroupingComparator() {
        super(StockTimeKey.class, true);
    }

    /**
     * 核心比较逻辑
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StockTimeKey k1 = (StockTimeKey) a;
        StockTimeKey k2 = (StockTimeKey) b;

        // 【关键点】只比较股票代码，完全忽略时间！
        return k1.getStockCode().compareTo(k2.getStockCode());
    }
}