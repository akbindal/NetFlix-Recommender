package ch.epfl.advadb.uviteration;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MreadVmapper implements Mapper<LongWritable, Text, IntWritable, Text> {
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	/*
	 * 
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 * input-> <mid:uid,Normalized_rat1:uid2,Normalized_rat2:....>
	 * output-> <mid, <'M':movieId,Norm_rat1:movieId2,Norm_rat2:...>>
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		// it will split into two tokens: userid , <moviedid, rat1:movied2, rat2:...>
		String[] tokens = line.split(":", 2);
		try { 
			int ui = Integer.parseInt(tokens[0]);
			output.collect(new IntWritable(ui), new Text("M:"+tokens[1]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
