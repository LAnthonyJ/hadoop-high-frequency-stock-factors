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
        // Validate the input and output paths
        if (args.length < 2) {
            System.err.println("Usage: MainDriver <InputPath> <OutputPath>");
            System.exit(-1);
        }

        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();// Job configuration
        // A 512 MB map-side sort buffer reduces spills and disk I/O.
        conf.set("mapreduce.task.io.sort.mb", "512");
        // Set JVM options for map tasks.
        // NewRatio=2 accommodates the short-lived Writable objects allocated during processing.
        conf.set("mapreduce.map.java.opts", "-Xms1536m -Xmx1536m -XX:NewRatio=2");
        // Reuse JVMs across tasks.
        conf.set("mapreduce.job.jvm.num.tasks", "-1");
        // Read snapshot files recursively from the input directory.
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        // Compress mapper output before the shuffle.
        conf.setBoolean("mapreduce.map.output.compress", true);
        // Snappy balances compression with CPU cost.
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Path inputPath = new Path(args[0]);
        Path finalOutputPath = new Path(args[1]);


        // Configure the job.
        // Create the job instance.
        Job job = Job.getInstance(conf, "Optimized_Factor_Job");
        job.setJarByClass(MainDriver.class);

        // Assign the mapper, combiner and reducer.
        job.setMapperClass(FactorMapper.class);
        job.setCombinerClass(FactorCombiner.class); // Aggregate locally before shuffling.
        // Combining snapshots for 300 stocks at each timestamp can reduce shuffle volume by over 90%.
        job.setReducerClass(FactorReducer.class);

        // Map output types
        // Each timestamp maps to a Writable containing sums for 20 factors.
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FactorSumCountWritable.class);

        // Final output types
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class); // Emit formatted CSV rows.

        // Use the configured reducer count to preserve timestamp ordering.
        job.setNumReduceTasks(1);

        // Combine small input files into roughly 512 MB splits on the four-core setup.
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 512 * 1024 * 1024);

        // Use lazy output creation.
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, finalOutputPath);

        // Postprocess the output.
        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);

        if (success) {
            // Finalize the CSV files in HDFS after the job succeeds.
            System.out.println("Job finished, starting post-processing...");
            postProcess(finalOutputPath, conf);

            long endTime = System.currentTimeMillis();// Record the completion time.
            System.out.println("Total Project Time: " + (endTime - startTime) + " ms");
        }
        // Exit with the job status.
        System.exit(success ? 0 : 1);
    }

    // Rename output files and add column headers in HDFS.
    private static void postProcess(Path outPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(outPath);

        // Build the CSV header.
        String header = "tradeTime,alpha_1,alpha_2,alpha_3,alpha_4,alpha_5,alpha_6,alpha_7,alpha_8,alpha_9,alpha_10," +
                "alpha_11,alpha_12,alpha_13,alpha_14,alpha_15,alpha_16,alpha_17,alpha_18,alpha_19,alpha_20\n";

        for (FileStatus f : files) {
            String name = f.getPath().getName();
            if (name.contains("-r-") && !name.startsWith(".")) {
                // Extract the date from the filename.
                String datePart = name.split("-")[0];
                Path finalCsvPath = new Path(outPath, datePart + ".csv");
                // Handle multiple output parts.
                boolean exists = fs.exists(finalCsvPath);

                try (FSDataInputStream in = fs.open(f.getPath());
                     FSDataOutputStream out = exists ? fs.append(finalCsvPath) : fs.create(finalCsvPath)) {

                    if (!exists) {
                        out.write(header.getBytes("UTF-8"));
                    }
                    IOUtils.copyBytes(in, out, conf, false);
                }
                // Remove the old temporary file in HDFS.
                fs.delete(f.getPath(), false);
            }
        }
        // Remove the job's _SUCCESS marker.
        fs.delete(new Path(outPath, "_SUCCESS"), false);
        System.out.println("All CSV files are ready in: " + outPath.toString());
    }
}