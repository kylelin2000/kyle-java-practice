package idv.kyle.practice.solr.mr;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class LuwakRecordReader_1 extends RecordReader<LongWritable, Text[]>
{	
	/**
	 * for index to Solr server performance consideration, cut multi-lines as a batch input to a mapper each time 
	 */
	String hive_delimieter = "";	
	
	final private int default_batchCount = 10;	
	private int batchCount = default_batchCount;
			
	private RecordReader<LongWritable, Text> helper = new LineRecordReader();
	private Text[] tmpValue = null;
	private List<Text> tmp_valList = new ArrayList<Text>();
	private Text[] value = null;
		
		/**
		 * Default constructor is needed when called by reflection from hadoop
		 * 
		 */		
		public LuwakRecordReader_1() 
		{
		}
						
		
		/**
		 * Called once at initialization.
		 */		
		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException
		{			
			/**
			 * Get setting from MR configuration
			 */
			Configuration job = context.getConfiguration();
			
			//get batch-count per map 
			this.batchCount = job.getInt(LuwakInputFormat.LINES_PER_MAPTASK, this.default_batchCount);
			
			this.tmpValue = new Text[this.batchCount];
			
			for(int i = 0; i < this.batchCount; i++)
			{
				this.tmpValue[i] = new Text();
				this.tmp_valList.add(this.tmpValue[i]);
			}				
						
			this.helper.initialize(genericSplit, context);		
		}
		
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException
		{			
			int count = 0;
			
			//reuse, clear old result
			for(Text text : this.tmpValue)
				text.set("");
			
			String oneRecord;									
			while((count < this.batchCount) && this.helper.nextKeyValue())
			{																			
				oneRecord = this.helper.getCurrentValue().toString();
								
				this.tmpValue[count].set(oneRecord);			
				
				count++;				
			}
				
			if (count == this.batchCount)
			{
				this.value = this.tmp_valList.toArray(new Text[0]);
			}
			else if (count > 0)
			{
				this.value = new Text[count];
				
				for(int i = 0; i < count; i++)				
					this.value[i] = new Text(this.tmpValue[i]);									
			}				
							
			return (count > 0); // Lwj : note: don't forget to return the last batch that MAY not reach the max batch count.						
		}		
		
		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException
		{			
			return this.helper.getCurrentKey();
		}

		
		@Override
		public Text[] getCurrentValue()  throws IOException, InterruptedException
		{
			return this.value;
		}		
		
		@Override
		public float getProgress()  throws IOException, InterruptedException
		{
			return this.helper.getProgress();			
		}
		
		@Override
		public synchronized void close() throws IOException 
		{	
			this.helper.close();		
		}				
}
