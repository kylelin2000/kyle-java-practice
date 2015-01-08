package idv.kyle.practice.solr.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;

import org.apache.http.impl.client.*;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.noggit.CharArr;


public class MRImportToSolr extends Configured implements Tool
{
	private Configuration conf = null;
	private FileSystem fs = null;
	public final static String conf_delimiter = "luwak_data_delimiter";
	//lwj : for Hadoop configuration can NOT some special delimiter. ex: ^A
	public final static String conf_delimiter_intVal = "luwak_data_delimiter_numeric";  
	public final static String conf_tags = "luwak_data_tags";
	public final static String conf_fields = "luwak_data_fields";
	
	public MRImportToSolr()
	{		
	}
	
	public MRImportToSolr(Configuration conf)
	{
		this.conf = conf;
		
		try {
			this.fs = FileSystem.get(this.conf);
		} 
		catch (IOException e) 
		{			
			e.printStackTrace();
		}
	}
		
	public boolean doSolrIndexder_idxByRaw(String srcPath, String tags) throws Exception
	{
		//tags
		this.conf.setStrings(MRImportToSolr.conf_tags,  tags);
						
		//for Solr performances consideration, set multi-records to one map each time		
		this.conf.setInt(LuwakInputFormat.LINES_PER_MAPTASK, 2000);
		
		/*
		 * create a new job		
		 */
		Job job = new Job(this.conf, "luwak to Solr index __ raw input job");
		job.setJarByClass(MRImportToSolr.class);
				
		LuwakInputFormat.setInputPaths(job, new Path(srcPath));
		job.setInputFormatClass(LuwakInputFormat.class);		
		
		String outputPath = "/tmp";
		Path path_output = new Path(outputPath);
        if (this.fs.exists(path_output))
        	this.fs.delete(path_output);	 		
       FileOutputFormat.setOutputPath(job, path_output);
       
		//set map & reduce format
		job.setMapperClass(MRImportToSolr.Luwak_SolrIndexer_RawInput_Mapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true);		
	}
	
	public static class Luwak_SolrIndexer_RawInput_Mapper extends Mapper<LongWritable, Text[], NullWritable, NullWritable>
	{
		/*localhost*/
		/*
		final String url = "http://localhost:8983/solr/luwak1";		
		String username = "alice";
		*/
		
		/*luwak-demo*/		
		final String url = "http://10.201.193.83:8983/solr/luwak1";		
		String username = "root";		
		
		private SolrServer server = null;
		
		final String rawField = "ALL_indexed";
		final String additionalfield_tag = "tag";
		
		String[] tags = null;		
		CharArr str_normalizedUtil = new CharArr();
		
		@Override
		protected void setup(Context context)
		{
			/**
			 * config Solr server											
			 */
			DefaultHttpClient httpClient = new DefaultHttpClient();							
				
			Credentials credentials = new UsernamePasswordCredentials(this.username);
			
			httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, credentials);						
																									
			this.server = new ConcurrentUpdateSolrServer(url, httpClient, 1000, 32);
			
													
			/**
			 * get configuration values
			 */
			Configuration conf = context.getConfiguration();						
			
			this.tags = conf.getStrings(MRImportToSolr.conf_tags);												
		}
		
		@Override
		protected void cleanup(Context context)
		{
			this.server.shutdown();
		}
									
		@Override
		protected void map(LongWritable key, Text value[], Context context) throws IOException, InterruptedException
		{			
			if (null == value || value.length == 0)
				return;
										
			//submit to Solr server by batch
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			int batchCount_local = 0;
			final int batchCount_max = 1000; 
			
			try 
			{	
				for(Text t : value)
				{										
					SolrInputDocument doc = new SolrInputDocument();
																			
					//add tag to indexer
					for (String tag : this.tags)						
						doc.addField(this.additionalfield_tag, tag);											
					
					//add content to indexer
					this.str_normalizedUtil.reset(); //reset
					
					Util.getUserReadableString(t.toString(), this.str_normalizedUtil);
					
					doc.addField(this.rawField, this.str_normalizedUtil.toString());
					
					//add the doc to document collection for batch execution
					docs.add(doc);
					
					if (batchCount_local == batchCount_max)
					{
						this.server.add(docs);
						
						//reset
						batchCount_local = 0;
						docs.clear();
					}				
				}
								
				if (docs.size() > 0)
					this.server.add(docs); //note : don't miss the last batch
				
				this.server.commit(); 
			} 
			catch (SolrServerException e) 
			{					
				e.printStackTrace();
				
				System.err.println(e.getMessage());
			}			
		}		
	}
	
	
	public boolean doSolrIndexder_idxByField(String srcPath, String delimiter, String fields, String tags) throws Exception
	{
		/**
		 * set fields information for MR job
		 */

		//delimiter : (Lwj) special handle for some character that cound not be pass to Hadoop configuration.. (ex :^A)
		if (delimiter.length() == 1)			
			this.conf.setInt(MRImportToSolr.conf_delimiter_intVal, (int)delimiter.charAt(0));
		else
			this.conf.set(MRImportToSolr.conf_delimiter, delimiter);		
		
		//fields
		//this.conf.setStrings(MRImportToSolr.conf_fields, fields);  //Lwj : conf.getStrings() will delete empty fields automatically...-__-
		this.conf.set(MRImportToSolr.conf_fields, fields);
	
		//tags
		this.conf.setStrings(MRImportToSolr.conf_tags, tags);

		//for Solr performances consideration, set multi-records to one map each time		
		this.conf.setInt(LuwakInputFormat.LINES_PER_MAPTASK, 2000);
				
		/**
		 * create a new job		
		 */
		Job job = new Job(this.conf, "luwak to Solr index  __ customized fields job");
		job.setJarByClass(MRImportToSolr.class);
				
		LuwakInputFormat.setInputPaths(job, new Path(srcPath));
		job.setInputFormatClass(LuwakInputFormat.class);		
		
		String outputPath = "/tmp";
		Path path_output = new Path(outputPath);
        if (this.fs.exists(path_output))
        	this.fs.delete(path_output);	 		
       FileOutputFormat.setOutputPath(job, path_output);
       
		//set map & reduce format
		job.setMapperClass(MRImportToSolr.Luwak_SolrIndexer_customizedFields_Mapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true);		
	}
	

	public static class Luwak_SolrIndexer_customizedFields_Mapper extends Mapper<LongWritable, Text[], NullWritable, NullWritable>
	{
		/*localhost*/	
		/*
		final String url = "http://localhost:8983/solr/luwak1";
		String username = "alice";		
		*/
		
		/*luwak-demo*/				
		final String url = "http://10.201.193.83:8983/solr/luwak1";		
		String username = "root";		
		
		private SolrServer server = null;
		
		String delimieter;
		final String hive_nullField_val = "\\N";												
		final String additionalfield_all = "ALL_noindexed";
		final String additionalfield_tag = "tag";
		final String additionalfield_datetime = "time";
		
		String[] tags = null;
		String[] fieldnames = null;
		StringBuilder sb = new StringBuilder();
		CharArr str_normalizedUtil = new CharArr(); 
		
		@Override
		protected void setup(Mapper.Context context)
		{
			/**
			 * config Solr server											
			 */											
			DefaultHttpClient httpClient = new DefaultHttpClient();
													
			Credentials credentials = new UsernamePasswordCredentials(this.username);
			
			httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, credentials);	
												
			this.server = new ConcurrentUpdateSolrServer(url, httpClient, 1000, 32);
																
			/**
			 * get configuration values
			 */
			Configuration conf = context.getConfiguration();
			
			String temp = conf.get(MRImportToSolr.conf_delimiter);
			if (null == temp)
			{
				int delimiter_val = conf.getInt(MRImportToSolr.conf_delimiter_intVal, 32767);
				if (delimiter_val != 32767)
					this.delimieter = Character.toString ((char) delimiter_val);
			}				
			else
			{
				this.delimieter = temp;
			}				 
						
			
			this.tags = conf.getStrings(MRImportToSolr.conf_tags);

			//this.fieldnames = conf.getStrings(MRImportToSolr.conf_fields); //Lwj : 'getStrings()' will automatically delete empty content... -_-
			this.fieldnames = conf.get(MRImportToSolr.conf_fields).split(",", -1);			
		}
		
		@Override
		protected void cleanup(Context context)
		{
			this.server.shutdown();
		}
				
					
		@Override
		protected void map(LongWritable key, Text value[], Context context) throws IOException, InterruptedException
		{
			if (null == value)
				return;
												
			int idx = -1;
			boolean doContentTransform = false;
			String fname, ftype;
			String dt;
			String[] fields;
			int x = value.length;
			
			//submit to Solr server by batch
			
			//Lwj : in case of collection of 'SolrInputDocument' grows too huge(memory or other resource)...
			final int batchCount_max = 1000;
			int batchCount_local = 0;
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			
			try 
			{	
				for(Text t : value)
				{										
					fields = t.toString().split(this.delimieter, -1);
					if (fields.length != this.fieldnames.length)
					{
						System.err.println("count of field not match count of field names!");
						continue;
					}
					
					doContentTransform = false; //reset
					
					batchCount_local++;
					
					SolrInputDocument doc = new SolrInputDocument();										
																			
					//1. add tag
					for (String tag : this.tags)						
						doc.addField(this.additionalfield_tag, tag);											
					
					//2. add field specified by user																									
					for (idx = 0; idx < this.fieldnames.length; idx++)
					{
						if (this.fieldnames[idx].length() == 0 || fields[idx].length() == 0)
							continue;
						
						if (this.fieldnames[idx].contains(":"))
						{														
							fname = this.fieldnames[idx].split(":", -1)[0];
							ftype = this.fieldnames[idx].split(":", -1)[1].trim();
							
							if (ftype.equalsIgnoreCase("IP_long"))
							{
								//transfer IP value from long to String							
							
								doContentTransform = true;
								
								fields[idx] = Util.IP_longToStr(Long.parseLong(fields[idx]));
								
								doc.addField(fname, fields[idx]);
							}
							else if (ftype.equalsIgnoreCase("unixtime"))
							{
								//transfer logtime value from long to 'Date' object
								
								doContentTransform = true;
								
								dt = Util.secToSolrDate(Long.parseLong(fields[idx]));   //sibling func : SQLDateToSolrDate()  :P
								
								doc.addField(fname, dt);
								
								fields[idx] = Util.SolrDateToSearchableFormat(dt);
								
								doc.addField(this.additionalfield_datetime, Util.SolrDateToSearchableFormat(dt));
							}
							else if (ftype.length() == 0 || ftype.equalsIgnoreCase("default"))
							{																
								doc.addField(fname, getUserReadableString(fields[idx]));
							}
							else
							{
								System.err.println("unknown field type!");
							}
						}
						else
						{
							doc.addField(this.fieldnames[idx], getUserReadableString(fields[idx]));	
						}
					}
					
					//3. add all content (for display)					
					if (doContentTransform)
					{
						this.sb.setLength(0);
						
						for(String f : fields)
						{
							this.sb.append(this.delimieter);
							this.sb.append(f);							
						}
												
						doc.addField(this.additionalfield_all, getUserReadableString(this.sb.substring(this.delimieter.length())));
					}
					else
					{
						doc.addField(this.additionalfield_all, getUserReadableString(t.toString()));
					}					
					
					docs.add(doc);
					
					if (batchCount_local == batchCount_max)
					{						
						this.server.add(docs);
						
						//reset
						batchCount_local = 0;
						docs.clear();
					}					
				}
				
				if (docs.size() > 0)								
					this.server.add(docs); //note : don't miss the last batch				
								
				this.server.commit(); 
			} 
			//catch (SolrServerException e) {
			catch (Exception e) 
			{				
				e.printStackTrace();
				System.err.println(e.getMessage());
			}			
		}							
		
		private String getUserReadableString(String rawStr)
		{
			this.str_normalizedUtil.reset(); //reset
			
			Util.getUserReadableString(rawStr, this.str_normalizedUtil);
			
			return this.str_normalizedUtil.toString();
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception 
	{
		String[] apSpicified_args = null;
		
		try 
		{			
			GenericOptionsParser cmdParser = new GenericOptionsParser(super.getConf(), args);
			
			apSpicified_args = cmdParser.getRemainingArgs();
		} 
		catch (IOException e1) 
		{
			e1.printStackTrace();
			
			System.exit(1);
		}
		
		if (apSpicified_args.length % 2 != 0)
			throw new Exception("argument count error! May be format error.");
		
		MRImportToSolr luwak_indexer = new MRImportToSolr(super.getConf());
				
		String srcPath = null, tags = null, delimiter = null, fields = null;  
		boolean rawinput = true;
		String argName, argVal;		
		for (int i = 0; i < apSpicified_args.length; i+=2)
		{
			argName = apSpicified_args[i];
			argVal = apSpicified_args[i+1];
			
			if (argName.equalsIgnoreCase("-srcPath"))
			{
				srcPath = argVal;
			}
			else if (argName.equalsIgnoreCase("-type"))
			{
				rawinput = argVal.equalsIgnoreCase("raw"); 
			}
			else if (argName.equalsIgnoreCase("-delimiter"))
			{
				delimiter = argVal;
				
				//Lwj : test from Eclipse
				if (argVal.equalsIgnoreCase("lwjTest"))
					delimiter =  Character.toString ((char) 1); //^A, default delimiter in hive output format
			}
			else if (argName.equalsIgnoreCase("-fields"))
			{
				fields = argVal;
			}
			else if (argName.equalsIgnoreCase("-tags"))
			{
				tags = argVal;
			}
		}		
		
		//do basic argument checking
		if (!rawinput && (null == delimiter || delimiter.length() == 0 || null == fields || fields.split(",", -1).length == 0))
			throw new Exception("please provide delimiter & fields information when executing solr-field-indexing!");
			
		if (rawinput && (null == tags || tags.length() == 0))
			throw new Exception("please provide tags when using raw indexing format!");
		
		boolean ret = false;
		if (rawinput)
			ret = luwak_indexer.doSolrIndexder_idxByRaw(srcPath, tags);
		else			
			ret = luwak_indexer.doSolrIndexder_idxByField(srcPath, delimiter, fields, tags);
		
		return (ret)? 0 : 1;
	}
			
	
	public static void main(String[] args) throws Exception 
	{	 	
		int ret = ToolRunner.run(new Configuration(), new MRImportToSolr(), args);
		  
		System.exit(ret);	 
	 }

}
