package reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import writable.FactorSumCountWritable;
import java.io.IOException;

public class FactorCombiner extends Reducer<LongWritable, FactorSumCountWritable, LongWritable, FactorSumCountWritable> {
    /**
     * Aggregate partial sums locally.
     * The key identifies a timestamp and the values contain per-stock factor totals from one map task.
     */
    @Override
    protected void reduce(LongWritable key, Iterable<FactorSumCountWritable> values, Context context)
            throws IOException, InterruptedException {

        FactorSumCountWritable combinedResult = new FactorSumCountWritable();
        // Merge accumulators with the same timestamp.
        for (FactorSumCountWritable val : values) {
            combinedResult.merge(val);
        }
        // Emit the combined totals for the shuffle.
        context.write(key, combinedResult);
    }
}