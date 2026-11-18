package job1;

import writable.RawDataWritable;
import writable.StockTimeKey;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义 Partitioner，确保同一只股票的数据发送到同一个 Reducer。
 * 这样，Reducer 才能处理这只股票的完整时间序列。
 */
public class StockPartitioner extends Partitioner<StockTimeKey, RawDataWritable> {

    @Override
    public int getPartition(StockTimeKey key, RawDataWritable value, int numReduceTasks) {
        // 使用股票代码的哈希值来决定分区。
        // `& Integer.MAX_VALUE` 是为了确保哈希值是正数，防止负数引起的问题。
        // `numReduceTasks` 是你在 Driver 中设置的 Reducer 数量。
        return (key.getStockCode().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}