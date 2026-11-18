package writable;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 聚合结果对象。
 * 存储某时刻所有股票的因子总和和股票数量
 * 用于计算最终的平均值: Avg=Sum / Count
 */
public class FactorSumCountWritable implements Writable {

    //存储20个因子的累加和
    public double[] sums = new double[20];
    //存储累加了多少只股票
    public int count = 0;

    public FactorSumCountWritable() {}

    /**
     * 将单只股票的计算结果加入累加器
     * @param factors 单只股票的 20 个因子数组
     */
    public void add(double[] factors) {
        for (int i = 0; i < 20; i++) {
            this.sums[i] += factors[i];
        }
        this.count++;
    }

    /**
     * 合并另一个累加器 (用于 Driver 端合并多个 Reducer 的输出)
     */
    public void merge(FactorSumCountWritable other) {
        for (int i = 0; i < 20; i++) {
            this.sums[i] += other.sums[i];
        }
        this.count += other.count;
    }

    /**
     * 计算平均值 (Driver输出最终CSV时使用)
     */
    public double[] calculateAverage() {
        double[] avg = new double[20];
        if (count > 0) {
            for (int i = 0; i < 20; i++) {
                avg[i] = sums[i] / count;
            }
        }
        return avg; // 如果 count=0，返回全0数组
    }
    /**
     * 序列化逻辑：将累加器的内部状态序列化为二进制流。
     * 先写入 4 字节的 count，紧接着顺序写入 20 个 8 字节的 double。
     * 这种固定长度且无冗余字段的设计，极大提升了 Hadoop 内部排序和拉取数据（Fetch）的效率。
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        for (double s : sums) {
            out.writeDouble(s);
        }
    }
    /**
     * 反序列化逻辑：从二进制流中恢复累加器的数据状态。
     * 必须保持与 write 方法完全镜像对称的读取顺序。
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        for (int i = 0; i < 20; i++) {
            sums[i] = in.readDouble();
        }
    }
    /**
     * 重写 toString 方法：
     * 仅用于调试和日志输出，展示当前对象聚合了多少只股票的数据。
     */
    @Override
    public String toString() {
        return "Count:" + count;
    }
}