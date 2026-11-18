package reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import writable.FactorSumCountWritable;
import java.io.IOException;

public class FactorCombiner extends Reducer<LongWritable, FactorSumCountWritable, LongWritable, FactorSumCountWritable> {
    /**
     * reduce 方法：实现局部数据的累加逻辑。
     * 参数 key 是时间戳，values 是当前 Map 任务中该时间戳对应的多个股票的计算结果。
     */
    @Override
    protected void reduce(LongWritable key, Iterable<FactorSumCountWritable> values, Context context)
            throws IOException, InterruptedException {

        FactorSumCountWritable combinedResult = new FactorSumCountWritable();
        //遍历当前节点输出的、具有相同时间戳的所有FactorSumCountWritable对象
        for (FactorSumCountWritable val : values) {
            combinedResult.merge(val);
        }
        //发送合并后的结果给Shuffle阶段
        context.write(key, combinedResult);
    }
}