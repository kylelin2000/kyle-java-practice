package idv.kyle.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

public class LoadHBase {
	public static void main(String[] args) throws IOException {
		Properties props = new Properties();
		props.setProperty("fs.default.name", "hdfs://sandbox:8020");
		props.setProperty("mapred.job.tracker", "sandbox:8020");
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE, props);
	}
}
