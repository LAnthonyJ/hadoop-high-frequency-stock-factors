package job1;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import writable.FactorSumCountWritable;
import java.io.IOException;

public class FactorReducer extends Reducer<LongWritable, FactorSumCountWritable, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> mos;

    // Track the averages at the previous timestamp.
    private double lastAvgAp1 = Double.NaN;   // Previous average best ask price
    private double lastAvgMid = Double.NaN;   // Previous average midpoint (F3)
    private double lastAvgRatio = Double.NaN; // Previous average depth ratio (F9)
    private long lastDate = -1;               // Detect a change of trading day.

    /**
     * Initialize resources once per reduce task.
     */
    @Override
    protected void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    /**
     * Process each second's market data in timestamp order.
     * The key uses YYYYMMDDHHMMSS. Values aggregate factors across stocks.
     */
    @Override
    protected void reduce(LongWritable key, Iterable<FactorSumCountWritable> values, Context context)
            throws IOException, InterruptedException {

        // Combine all records for this timestamp.
        FactorSumCountWritable finalAgg = new FactorSumCountWritable();
        for (FactorSumCountWritable val : values) {
            finalAgg.merge(val);
            // Merge the sums of all 20 factors and their observation counts.
        }

        // Compute the current averages.
        double[] avgs = finalAgg.calculateAverage();

        // Compute factors 17–19 from changes in market averages.
        long tradeTime = key.get();  // Full timestamp
        long currentDate = tradeTime / 1000000L; // Extract the trading date.

        // Reset the previous values at the start of a trading day.
        if (currentDate != lastDate) {
            lastAvgAp1 = Double.NaN;
            lastAvgMid = Double.NaN;
            lastAvgRatio = Double.NaN;
            lastDate = currentDate;
        }

        // Recover the current average best ask.
        // Reconstruct the best ask from the factors that the mapper emits.
        double currAvgF1 = avgs[0]; // Spread
        double currAvgF3 = avgs[2]; // Mid
        double currAvgF9 = avgs[8]; // Ratio

        double currAvgAp1 = currAvgF3 + 0.5 * currAvgF1;

        // Compute the changes since the previous timestamp.
        if (!Double.isNaN(lastAvgAp1)) {
            // F17: change in average best ask
            avgs[16] = currAvgAp1 - lastAvgAp1;
            // F18: change in average midpoint
            avgs[17] = currAvgF3 - lastAvgMid;
            // F19: change in average depth ratio
            avgs[18] = currAvgF9 - lastAvgRatio;
        } else {
            // Use zero for the first observation of the day.
            avgs[16] = 0.0;
            avgs[17] = 0.0;
            avgs[18] = 0.0;
        }

        // Retain the current values for the next second.
        lastAvgAp1 = currAvgAp1;
        lastAvgMid = currAvgF3;
        lastAvgRatio = currAvgF9;

        // Format the output row.
        long hhmmss = key.get() % 1000000;// Extract HHMMSS.
        // Apply the trading-hours filter.
        if (hhmmss >= 93000) {
            StringBuilder sb = new StringBuilder();
            // Format time as six digits.
            sb.append(String.format("%06d", hhmmss));

            // Write the 20 factor means to ten decimal places.
            for (double avg : avgs) {
                sb.append(",").append(String.format("%.10f", avg));
            }

            // Choose an output filename for the trading date.
            String dateStr = String.format("%04d", (tradeTime / 1000000) % 10000);
            mos.write(NullWritable.get(), new Text(sb.toString()), dateStr);
        }
    }

    /**
     * Release resources at task completion.
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}