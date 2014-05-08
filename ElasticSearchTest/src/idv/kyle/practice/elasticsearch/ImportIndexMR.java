package idv.kyle.practice.elasticsearch;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class ImportIndexMR {
	public class MyMapper extends MapReduceBase implements Mapper {
		 @Override
		 public void map(Object key, Object value, OutputCollector output,
		                    Reporter reporter) throws IOException {
		   // create the MapWritable object
		   MapWritable doc = new MapWritable();
		   doc.put(new Text("id"), new Text("id2"));
		   
		   // write the result to the output collector
		   // one can pass whatever value to the key; EsOutputFormat ignores it
		   output.collect(NullWritable.get(), doc);
		 }}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ImportIndexMR.class);
		conf.setJobName("ImportDocToElasticSearch");
		
		conf.setSpeculativeExecution(false);           
		conf.set("es.nodes", "es1:9200");
		conf.set("es.resource", "user100/test");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(EsOutputFormat.class);
		conf.setMapOutputValueClass(MapWritable.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}