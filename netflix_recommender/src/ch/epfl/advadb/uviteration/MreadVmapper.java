package ch.epfl.advadb.uviteration;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advadb.IO.TupleTriplet;

public class MreadVmapper implements Mapper<LongWritable, Text, IntWritable, TupleTriplet> {
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
	 * input-> input-> <uid:movieid1,Normalized_rat1:movieid2,Normalized_rat2:....>
	 * output-> <mid, <'M':uid,Norm_rat1>>
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, TupleTriplet> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		// it will split into two tokens: userid , <moviedid, rat1:movied2, rat2:...>
		String[] tokens = line.split(":", 2);
		try { 
			int ui = Integer.parseInt(tokens[0]);
			String[] pair = tokens[1].split(",");
			int mi = Integer.parseInt(pair[0]);
			float rat = Float.parseFloat(pair[1]);
			
			output.collect(new IntWritable(mi), new TupleTriplet('M', ui, rat)); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
