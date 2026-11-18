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

    //状态变量，用于记录上一时刻的均值
    private double lastAvgAp1 = Double.NaN;   // 上一时刻的平均卖一价
    private double lastAvgMid = Double.NaN;   // 上一时刻的平均中间价 (F3)
    private double lastAvgRatio = Double.NaN; // 上一时刻的平均深度比 (F9)
    private long lastDate = -1;               // 检测是否跨天

    /**
     * setup初始化方法：在Reducer任务启动时执行一次。
     */
    @Override
    protected void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    /**
     * reduce方法：按时间戳顺序处理每一秒的全市场数据。
     * 参数key是fullTime(YYYYMMDDHHMMSS)，values是所有股票该时刻因子数据的累加器。
     */
    @Override
    protected void reduce(LongWritable key, Iterable<FactorSumCountWritable> values, Context context)
            throws IOException, InterruptedException {

        // 1. 聚合当前时刻的数据
        FactorSumCountWritable finalAgg = new FactorSumCountWritable();
        for (FactorSumCountWritable val : values) {
            finalAgg.merge(val);
            //merge会将20个因子的sums数组累加，并将count计数器累加
        }

        // 2. 计算当前时刻的基础平均值
        double[] avgs = finalAgg.calculateAverage();

        // 3. 利用“平均值的差”补全 Alpha 17-19
        long tradeTime = key.get();  //获取当前记录的完整时间戳
        long currentDate = tradeTime / 1000000L; // 提取日期

        // 如果跨天了，重置状态
        if (currentDate != lastDate) {
            lastAvgAp1 = Double.NaN;
            lastAvgMid = Double.NaN;
            lastAvgRatio = Double.NaN;
            lastDate = currentDate;
        }

        // 3.1 还原当前时刻的Avg(Ap1)
        //由于传输对象中未直接包含原始卖一价(Ap1)，我们需要利用已知因子进行代数反解
        double currAvgF1 = avgs[0]; // Spread
        double currAvgF3 = avgs[2]; // Mid
        double currAvgF9 = avgs[8]; // Ratio

        double currAvgAp1 = currAvgF3 + 0.5 * currAvgF1;

        // 3.2 计算差分因子 (Diff)
        if (!Double.isNaN(lastAvgAp1)) {
            //Alpha 17: 平均卖一价变动
            avgs[16] = currAvgAp1 - lastAvgAp1;
            //Alpha 18: 平均中间价变动
            avgs[17] = currAvgF3 - lastAvgMid;
            //Alpha 19: 平均深度比变动
            avgs[18] = currAvgF9 - lastAvgRatio;
        } else {
            //第一条数据，变动设为0
            avgs[16] = 0.0;
            avgs[17] = 0.0;
            avgs[18] = 0.0;
        }

        // 3.3 更新状态，供下一秒使用
        lastAvgAp1 = currAvgAp1;
        lastAvgMid = currAvgF3;
        lastAvgRatio = currAvgF9;

        // 4. 格式化输出
        long hhmmss = key.get() % 1000000;//提取HHMMSS
        //过滤开盘之后时间
        if (hhmmss >= 93000) {
            StringBuilder sb = new StringBuilder();
            //格式化时间为 6 位数字
            sb.append(String.format("%06d", hhmmss));

            //遍历20个因子均值，转换为CSV要求的格式，保留10位小数以确保精度得分
            for (double avg : avgs) {
                sb.append(",").append(String.format("%.10f", avg));
            }

            //根据当前日期生成输出文件名
            String dateStr = String.format("%04d", (tradeTime / 1000000) % 10000);
            mos.write(NullWritable.get(), new Text(sb.toString()), dateStr);
        }
    }

    /**
     * cleanup 资源销毁方法：在 Reducer 任务结束前执行。
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}