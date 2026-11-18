package job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.FactorCalculator;
import writable.FactorSumCountWritable;
import writable.RawDataWritable;

import java.io.IOException;

public class FactorMapper extends Mapper<LongWritable, Text, LongWritable, FactorSumCountWritable> {

    //复用输出Key对象，避免每行数据都实例化一个新的LongWritable
    private final LongWritable outKey = new LongWritable();
    //复用输出Value对象，显著降低Young Generation的GC压力
    private final FactorSumCountWritable outValue = new FactorSumCountWritable();

    //复用一个RawDataWritable，用于向FactorCalculator传递解析结果
    private final RawDataWritable tempRawData = new RawDataWritable();

    //临时数组，用于存储解析出的5档数据，避免在map方法内重复申请内存空间
    private final double[] apBuffer = new double[5];
    private final double[] bpBuffer = new double[5];
    private final double[] avBuffer = new double[5];
    private final double[] bvBuffer = new double[5];

    //用于暂存计算器产出的 20 个因子结果
    private final double[] factorsBuffer = new double[20];

    /**
     * map 方法：实现单行记录的流式处理逻辑。
     * 遵循“解析 - 过滤 - 计算 - 发送”的流水线模式。
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        //跳过CSV表头行以及空行
        if (line.startsWith("tradingDay") || line.isEmpty()) {
            return;
        }

        //手动索引跳转，和split方法相比不会产生中间字符串数组对象
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
                //根据逗号数量定位业务相关列
                if (commaCount == 0) dateVal = parseLongFast(line, start, i);//日期
                else if (commaCount == 1) timeVal = parseLongFast(line, start, i);//时间
                else if (commaCount == 4) stockCode = line.substring(start, i);
                else if (commaCount == 12) tBidVol = parseDoubleFast(line, start, i);
                else if (commaCount == 13) tAskVol = parseDoubleFast(line, start, i);
                //批量提取5档行情数据
                else if (commaCount >= 17 && commaCount <= 36) {
                    int rel = commaCount - 17;
                    int level = rel / 4;
                    int type = rel % 4;
                    double val = parseDoubleFast(line, start, i);
                    //填充至对应的极速缓冲区数组中
                    if (type == 0) bpBuffer[level] = val;
                    else if (type == 1) bvBuffer[level] = val;
                    else if (type == 2) apBuffer[level] = val;
                    else if (type == 3) avBuffer[level] = val;
                }
                start = i + 1;
                commaCount++;
                //一旦解析完计算所需的最后一列，立即停止遍历后续字段
                if (commaCount > 36) break;
            }
        }

        if (stockCode != null) {
            // 时间过滤
            if (timeVal < 92950 || timeVal > 150000) {
                return;
            }

            //调用工具类计算
            //将解析出的Buffer注入RawDataWritable容器
            tempRawData.set(apBuffer, bpBuffer, avBuffer, bvBuffer, tAskVol, tBidVol);

            //调用FactorCalculator
            //由于Mapper是乱序处理的，时序因子(F17-19)暂时传入 NaN 忽略
            FactorCalculator.calculate(tempRawData, factorsBuffer, Double.NaN, Double.NaN, Double.NaN);

            // 合成全局唯一时间键：YYYYMMDD * 1000000 + HHMMSS
            long fullTime = dateVal * 1000000L + timeVal;
            outKey.set(fullTime);

            //封装结果并输出
            outValue.sums = factorsBuffer;
            outValue.count = 1;
            //将计算结果发送至Shuffle阶段，后续将由Combiner进行预聚合
            context.write(outKey, outValue);
        }
    }

    /**
     * 辅助解析方法：parseLongFast
     * 封装标准库的解析逻辑，增加健壮性处理，防止格式异常导致整个 Map 任务崩溃。
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
     * 辅助解析方法：parseDoubleFast
     * 用于处理浮点型字段（如价格、成交量）。
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