package idv.kyle.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertTextToSequence extends
    Mapper<LongWritable, Text, LongWritable, Text> {
  static final Logger logger = LoggerFactory.getLogger(ConvertTextToSequence.class);

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length != 2) {
      System.out.println("Examples: executablejarfile /tmp/install.log /tmp/install.log.seq");
      return;
    }
    logger.info("kyle: info");
    logger.debug("kyle: debug");
    logger.warn("kyle: warn");
    logger.trace("kyle: trace");
    Configuration conf = new Configuration();
    Job job = new Job(conf);
    job.setJobName("Convert Text");
    job.setJarByClass(ConvertTextToSequence.class);

    job.setMapperClass(Mapper.class);
    job.setReducerClass(Reducer.class);

    job.setNumReduceTasks(0);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // submit and wait for completion
    job.waitForCompletion(true);
  }
}
