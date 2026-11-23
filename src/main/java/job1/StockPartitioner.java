package job1;

import writable.RawDataWritable;
import writable.StockTimeKey;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Assign all records for a stock to the same reducer.
 * Each reducer can then process that stock's full time series.
 */
public class StockPartitioner extends Partitioner<StockTimeKey, RawDataWritable> {

    @Override
    public int getPartition(StockTimeKey key, RawDataWritable value, int numReduceTasks) {
        // Partition by the stock code hash.
        // Mask the sign bit to obtain a nonnegative hash.
        // The driver supplies the number of reduce tasks.
        return (key.getStockCode().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}