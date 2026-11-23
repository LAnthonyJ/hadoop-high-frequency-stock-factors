package job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.FactorCalculator;
import writable.FactorSumCountWritable;
import writable.RawDataWritable;

import java.io.IOException;

public class FactorMapper extends Mapper<LongWritable, Text, LongWritable, FactorSumCountWritable> {

    // Reuse the output key across input rows.
    private final LongWritable outKey = new LongWritable();
    // Reuse the output value to reduce allocation pressure.
    private final FactorSumCountWritable outValue = new FactorSumCountWritable();

    // Reuse the input snapshot passed to FactorCalculator.
    private final RawDataWritable tempRawData = new RawDataWritable();

    // Reuse arrays for the five book levels.
    private final double[] apBuffer = new double[5];
    private final double[] bpBuffer = new double[5];
    private final double[] avBuffer = new double[5];
    private final double[] bvBuffer = new double[5];

    // Buffer for the 20 calculated factors.
    private final double[] factorsBuffer = new double[20];

    /**
     * Process one input record at a time.
     * Parse, filter and calculate before emitting the record.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip empty rows and the CSV header.
        if (line.startsWith("tradingDay") || line.isEmpty()) {
            return;
        }

        // Walk comma offsets directly to avoid an intermediate string array.
        int len = line.length();
        int commaCount = 0;
        int start = 0;

        long dateVal = 0;
        long timeVal = 0;
        String stockCode = null;
        double tBidVol = 0;
        double tAskVol = 0;

        for (int i = 0; i < len; i++) {
            if (line.charAt(i) == ',') {
                // Match input columns by comma count.
                if (commaCount == 0) dateVal = parseLongFast(line, start, i);// Date
                else if (commaCount == 1) timeVal = parseLongFast(line, start, i);// Time
                else if (commaCount == 4) stockCode = line.substring(start, i);
                else if (commaCount == 12) tBidVol = parseDoubleFast(line, start, i);
                else if (commaCount == 13) tAskVol = parseDoubleFast(line, start, i);
                // Extract the first five price and volume levels.
                else if (commaCount >= 17 && commaCount <= 36) {
                    int rel = commaCount - 17;
                    int level = rel / 4;
                    int type = rel % 4;
                    double val = parseDoubleFast(line, start, i);
                    // Copy each field to its buffer.
                    if (type == 0) bpBuffer[level] = val;
                    else if (type == 1) bvBuffer[level] = val;
                    else if (type == 2) apBuffer[level] = val;
                    else if (type == 3) avBuffer[level] = val;
                }
                start = i + 1;
                commaCount++;
                // Stop after reading the last required column.
                if (commaCount > 36) break;
            }
        }

        if (stockCode != null) {
            // Filter by trade time.
            if (timeVal < 92950 || timeVal > 150000) {
                return;
            }

            // Compute snapshot factors.
            // Pass the parsed buffers to the snapshot object.
            tempRawData.set(apBuffer, bpBuffer, avBuffer, bvBuffer, tAskVol, tBidVol);

            // Call FactorCalculator.
            // Mapper input is unordered. Pass NaN for previous values and compute F17–F19 later.
            FactorCalculator.calculate(tempRawData, factorsBuffer, Double.NaN, Double.NaN, Double.NaN);

            // Build the key as YYYYMMDD * 1000000 + HHMMSS.
            long fullTime = dateVal * 1000000L + timeVal;
            outKey.set(fullTime);

            // Package and emit the result.
            outValue.sums = factorsBuffer;
            outValue.count = 1;
            // The combiner aggregates local sums before the shuffle.
            context.write(outKey, outValue);
        }
    }

    /**
     * Parse an integer field.
     * Handle parse failures without aborting the map task.
     */
    private long parseLongFast(String line, int start, int end) {
        if (start >= end) return 0L;
        try {
            return Long.parseLong(line.substring(start, end));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * Parse a floating-point field.
     * Used for prices and volumes.
     */
    private double parseDoubleFast(String line, int start, int end) {
        if (start >= end) return 0.0;
        try {
            return Double.parseDouble(line.substring(start, end));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}