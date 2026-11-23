package writable;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Data exchanged between the mapper and reducer in Job 1.
 * Keep five levels of bid and ask prices and volumes, plus the totals needed for the 20 factors.
 */
public class RawDataWritable implements Writable, Cloneable {

    // Store the first five levels in arrays.
    private double[] askPrices = new double[5];   // Ask prices ap1–ap5
    private double[] bidPrices = new double[5];   // Bid prices bp1–bp5
    private double[] askVolumes = new double[5];  // Ask volumes av1–av5
    private double[] bidVolumes = new double[5];  // Bid volumes bv1–bv5

    private double totalAskVol; // Total ask volume
    private double totalBidVol; // Total bid volume

    public RawDataWritable() {
    }

    /**
     * Populate the snapshot from the mapper's reusable buffers.
     */
    public void set(double[] ap, double[] bp, double[] av, double[] bv, double tAsk, double tBid) {
        // Transfer the parsed level data to the snapshot arrays.
        System.arraycopy(ap, 0, this.askPrices, 0, 5);
        System.arraycopy(bp, 0, this.bidPrices, 0, 5);
        System.arraycopy(av, 0, this.askVolumes, 0, 5);
        System.arraycopy(bv, 0, this.bidVolumes, 0, 5);
        this.totalAskVol = tAsk;
        this.totalBidVol = tBid;
    }
    /**
     * Serialize twenty-two double values.
     * Write the values for each of the five levels.
     * The binary record does not repeat field names.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // Write each book level in order.
        for (int i = 0; i < 5; i++) {
            out.writeDouble(askPrices[i]);
            out.writeDouble(bidPrices[i]);
            out.writeDouble(askVolumes[i]);
            out.writeDouble(bidVolumes[i]);
        }
        // Write aggregate totals.
        out.writeDouble(totalAskVol);
        out.writeDouble(totalBidVol);
    }
    /**
     * Restore prices and volumes for the five levels.
     * Keep read order consistent with write().
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // Read fields in their serialized order.
        for (int i = 0; i < 5; i++) {
            askPrices[i] = in.readDouble();
            bidPrices[i] = in.readDouble();
            askVolumes[i] = in.readDouble();
            bidVolumes[i] = in.readDouble();
        }
        totalAskVol = in.readDouble();
        totalBidVol = in.readDouble();
    }

    // Accessors for FactorCalculator.
    public double[] getAskPrices() { return askPrices; }
    public double[] getBidPrices() { return bidPrices; }
    public double[] getAskVolumes() { return askVolumes; }
    public double[] getBidVolumes() { return bidVolumes; }
    public double getTotalAskVol() { return totalAskVol; }
    public double getTotalBidVol() { return totalBidVol; }
    /**
     * Format the snapshot for diagnostics.
     * Include the best ask and bid to help inspect the data stream.
     */
    @Override
    public String toString() {
        return "RawData{" +
                "ap1=" + askPrices[0] +
                ", bp1=" + bidPrices[0] +
                '}';
    }


}