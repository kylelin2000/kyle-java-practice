package idv.kyle.practice.solr.mr;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Configurable CSV line reader. Variant of NLineInputReader that reads CSV
 * lines, even if the CSV has multiple lines inside a single column. Also
 * implements the getSplits method so splits are made by lines
 * 
 *  
 * 
 */
public class LuwakInputFormat extends FileInputFormat<LongWritable, Text> 
{
	public static final String LINES_PER_MAPTASK = "mapreduce.batch_lines";
	public static final String FIELDS_PER_RECORD = "mapreduce.record.fieldcount";
	
	@Override
	public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException 
	{							
		return new LuwakRecordReader_1();
	}
}
