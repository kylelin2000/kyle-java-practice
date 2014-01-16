package idv.kyle.hbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class HBaseDeleteMapReduce {
	static Logger LOG = Logger.getLogger(HBaseDeleteMapReduce.class.getName());
	
	public HBaseDeleteMapReduce() throws FileNotFoundException, IOException {
	}
	
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, Delete> {
		@Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			Runtime runtime = Runtime.getRuntime();
		    LOG.info("memory free: " + runtime.freeMemory() + ", total: " + runtime.totalMemory() + ", max memory: " + runtime.maxMemory());
		    context.write(row, new Delete(row.get()));
        }
	}

	public void delete(String docNo) throws IOException, InterruptedException, ClassNotFoundException {
		Date startDt = new Date();
		LOG.debug("Do MapReduce. Start. " + startDt);
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "host3");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("mapred.job.tracker", "host3:9001");
		config.set("fs.default.name", "hdfs://host3:9000");
		config.set("mapred.child.java.opts", "-Xmx512m");
		
		config.set("DOC_NO", docNo);
		config.set("HBASE_COLUMN_FAMILY", "rim_cf");
		String tableName = "TEST_RCP_BODY";
		config.set("TABLE_NAME", tableName);
		
		Job job = new Job(config,"HBaseDeleteByDocNo_" + docNo);
		job.setJarByClass(HBaseDeleteMapReduce.class);     // class that contains mapper and reducer
		
		Scan scan = new Scan();
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		//scan.setFilter(new PrefixFilter(docNo.getBytes()));
		String[] tokens = docNo.split("-");
		int upperBond = Integer.parseInt(tokens[1]) + 1;
		scan.setStartRow(Bytes.toBytes(docNo));
		scan.setStopRow(Bytes.toBytes(tokens[0] + "-" + upperBond));
		
		LOG.info("Do Delete!");
		TableMapReduceUtil.initTableMapperJob(
			tableName,        // input table
			scan,               // Scan instance to control CF and attribute selection
			MyMapper.class,     // mapper class
			null,         // mapper output key
			null,  // mapper output value
			job);
		
		job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		
		job.setNumReduceTasks(0);
		String filePath = "user/user1/delete_" + docNo;
		Path outFolder = new Path(filePath);
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(outFolder)){
			// if folder exists, delete first
			fs.delete(outFolder, true);
		}
		FileOutputFormat.setOutputPath(job, outFolder);  // adjust directories as required
			    
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("Error occured in Map Reduce!!! Please see logs under HADOOP_HOME/logs/userlogs/");
		} else {
			//do nothing
		}
		
		Date endDt = new Date();
		LOG.info("Do MapReduce. Finished. Cost millisec:" + (endDt.getTime() - startDt.getTime()));
	}
	
	public static void main(String[] args) throws Exception {
		HBaseDeleteMapReduce deleting = new HBaseDeleteMapReduce();
		deleting.delete("4101B-2002");
	}
}
