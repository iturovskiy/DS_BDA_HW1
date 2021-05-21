package hw1;

import hw1.mapper.AppMapper;
import hw1.reducer.AppReducer;
import hw1.custom.KeyMapper;
import hw1.custom.KeyReducer;

import lombok.extern.log4j.Log4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


@Log4j
public class MapReduceApplication {

    /**
     * Entry point for the application
     *
     * @param args Optional arguments: InputDirectory, OutputDirectory, Interval
     * @throws Exception if job fails
     */
    public static void main(final String[] args) throws Exception {

        Configuration configuration = new Configuration();
        if (args.length != 3) {
            log.error("Usage: InputDirectory OutputDirectory Interval");
            System.exit(2);
        }
        String inputDir = args[0];
        String outputDir = args[1];

        configuration.set("interval", args[2]);

        Job job = Job.getInstance(configuration);

        job.setJarByClass(MapReduceApplication.class);

        job.setMapperClass(AppMapper.class);
        job.setReducerClass(AppReducer.class);

        job.addCacheFile(new Path("hdfs://localhost:9000/user/root/input/mapping").toUri());

        job.setMapOutputKeyClass(KeyMapper.class);
        job.setMapOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.setOutputKeyClass(KeyReducer.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        long total = 0;
        Counter counter = job.getCounters().findCounter(hw1.util.Counter.CORRUPTED_ROW);
        log.info("=====================COUNTER " + counter.getName() + ": "
                + counter.getValue() + "=====================");
        total += counter.getValue();
        counter = job.getCounters().findCounter(hw1.util.Counter.CORRUPTED_DEV);
        log.info("=====================COUNTER " + counter.getName() + ": "
                + counter.getValue() + "=====================");
        total += counter.getValue();
        counter = job.getCounters().findCounter(hw1.util.Counter.CORRUPTED_TIM);
        log.info("=====================COUNTER " + counter.getName() + ": "
                + counter.getValue() + "=====================");
        total += counter.getValue();
        log.info("=====================TOTAL CORRUPTED ROWS: " + total + "=====================");
    }
}
