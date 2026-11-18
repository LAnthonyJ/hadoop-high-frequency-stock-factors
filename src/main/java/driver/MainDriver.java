package driver;

import job1.*;
import reducer.FactorCombiner;
import job1.FactorReducer;
import writable.FactorSumCountWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MainDriver {

    public static void main(String[] args) throws Exception {
        // 校验参数，确保输入路径和输出路径已通过命令行传入
        if (args.length < 2) {
            System.err.println("Usage: MainDriver <InputPath> <OutputPath>");
            System.exit(-1);
        }

        //初始化 Hadoop 全局配置对象
        Configuration conf = new Configuration();//hadoop初始化
        //增大Map端环形排序缓冲区：设置为512MB，减少溢写次数，降低磁盘IO
        conf.set("mapreduce.task.io.sort.mb", "512");
        //配置Map任务JVM参数
        //-XX:NewRatio=2调整年轻代比例，因为计算过程会产生大量临时Writable对象，优化GC频率。
        conf.set("mapreduce.map.java.opts", "-Xms1536m -Xmx1536m -XX:NewRatio=2");
        //复用JVM进程
        conf.set("mapreduce.job.jvm.num.tasks", "-1");
        //递归读取输入目录，从多级子文件夹中自动搜寻并加载行情快照原始数据文件
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        // 启用Map输出压缩，减少从Mapper到Reducer跨网络传输的数据量
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 使用Snappy算法，压缩比与CPU消耗之间达到了极佳平衡
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Path inputPath = new Path(args[0]);
        Path finalOutputPath = new Path(args[1]);


        //Job配置
        //实例化Job对象
        Job job = Job.getInstance(conf, "Optimized_Factor_Job");
        job.setJarByClass(MainDriver.class);

        //设置Mapper,Combiner和Reducer
        job.setMapperClass(FactorMapper.class);
        job.setCombinerClass(FactorCombiner.class); //Map端预聚合
        // 由于CSV文件内300只股票在同一时刻的数据量很大，此举能将发往Reducer的数据压缩90%以上
        job.setReducerClass(FactorReducer.class);

        //Map阶段输出的Key/Value类型
        //Key为时间戳LongWritable，Value包含20档因子累加和的自定义Writable对象
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FactorSumCountWritable.class);

        //Reduce阶段最终输出的Key/Value类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class); // 直接输出格式化好的CSV行

        //并发设置，确保时间排序正常
        job.setNumReduceTasks(1);

        //输入格式：合并小文件，针对4核环境切片设为512MB左右最均衡
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 512 * 1024 * 1024);

        //输出格式：使用LazyOutputFormat
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, finalOutputPath);

        //后处理
        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);

        if (success) {
            //计算完成后，执行轻量级的HDFS后处理
            System.out.println("Job finished, starting post-processing...");
            postProcess(finalOutputPath, conf);

            long endTime = System.currentTimeMillis();// 记录结束时间
            System.out.println("Total Project Time: " + (endTime - startTime) + " ms");
        }
        //根据执行状态码安全退出JVM
        System.exit(success ? 0 : 1);
    }

    //在HDFS上完成文件重命名和表头添加
    private static void postProcess(Path outPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(outPath);

        //定义CSV表头字符串
        String header = "tradeTime,alpha_1,alpha_2,alpha_3,alpha_4,alpha_5,alpha_6,alpha_7,alpha_8,alpha_9,alpha_10," +
                "alpha_11,alpha_12,alpha_13,alpha_14,alpha_15,alpha_16,alpha_17,alpha_18,alpha_19,alpha_20\n";

        for (FileStatus f : files) {
            String name = f.getPath().getName();
            if (name.contains("-r-") && !name.startsWith(".")) {
                //提取文件名中的日期部分
                String datePart = name.split("-")[0];
                Path finalCsvPath = new Path(outPath, datePart + ".csv");
                //处理多副本写入情况
                boolean exists = fs.exists(finalCsvPath);

                try (FSDataInputStream in = fs.open(f.getPath());
                     FSDataOutputStream out = exists ? fs.append(finalCsvPath) : fs.create(finalCsvPath)) {

                    if (!exists) {
                        out.write(header.getBytes("UTF-8"));
                    }
                    IOUtils.copyBytes(in, out, conf, false);
                }
                //删除旧的HDFS临时文件
                fs.delete(f.getPath(), false);
            }
        }
        // 清理作业产生的 _SUCCESS 文件
        fs.delete(new Path(outPath, "_SUCCESS"), false);
        System.out.println("All CSV files are ready in: " + outPath.toString());
    }
}