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

public class VReadMapper implements Mapper<LongWritable, Text, IntWritable, TupleTriplet>{
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	 
	/**
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 * input <V, [1..10], vid, value>
	 * output <vid, <[1..10]:value>>
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, TupleTriplet> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String[] tokens = line.split(",");
		//StringTokenizer itr = new StringTokenizer(line, ":");
		try { 
			String vid = tokens[2];//titr.nextToken();
			int vi = Integer.parseInt(vid);
			int fi = Integer.parseInt(tokens[1]);
			float fv = Float.parseFloat(tokens[3]);
			output.collect(new IntWritable(vi), new TupleTriplet('V', fi, fv));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
