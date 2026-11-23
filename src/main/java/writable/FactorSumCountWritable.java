package writable;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Accumulator for a single timestamp.
 * Keep the sum of each factor and the number of contributing stocks.
 * Divide sums by the observation count to obtain averages.
 */
public class FactorSumCountWritable implements Writable {

    // Sums for 20 factors.
    public double[] sums = new double[20];
    // Number of contributing stocks.
    public int count = 0;

    public FactorSumCountWritable() {}

    /**
     * Add one stock's factors to the accumulator.
     * @param factors array of 20 factors for one stock
     */
    public void add(double[] factors) {
        for (int i = 0; i < 20; i++) {
            this.sums[i] += factors[i];
        }
        this.count++;
    }

    /**
     * Merge another accumulator, including when combining reducer output.
     */
    public void merge(FactorSumCountWritable other) {
        for (int i = 0; i < 20; i++) {
            this.sums[i] += other.sums[i];
        }
        this.count += other.count;
    }

    /**
     * Compute averages for the final CSV output.
     */
    public double[] calculateAverage() {
        double[] avg = new double[20];
        if (count > 0) {
            for (int i = 0; i < 20; i++) {
                avg[i] = sums[i] / count;
            }
        }
        return avg; // Return zeros when there are no observations.
    }
    /**
     * Serialize the accumulator to a binary stream.
     * Write the four-byte count followed by twenty eight-byte doubles.
     * The fixed-width layout avoids redundant fields during sort and fetch.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        for (double s : sums) {
            out.writeDouble(s);
        }
    }
    /**
     * Restore the accumulator from the stream.
     * Read fields in the same order as write().
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        for (int i = 0; i < 20; i++) {
            sums[i] = in.readDouble();
        }
    }
    /**
     * Format the accumulator for debugging.
     * Include the number of stocks represented in log messages.
     */
    @Override
    public String toString() {
        return "Count:" + count;
    }
}