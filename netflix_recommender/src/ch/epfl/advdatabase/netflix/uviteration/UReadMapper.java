package ch.epfl.advdatabase.netflix.uviteration;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UReadMapper implements Mapper<LongWritable, Text, IntWritable, Text>{

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	 
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 * input <uid:uf1,uf2>
	 * output <uid, <'U':uf1,uf2...>>
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String[] tokens = line.split(":");
		//StringTokenizer itr = new StringTokenizer(line, ":");
		try { 
			String uid = tokens[0];
			int ui = Integer.parseInt(uid);
			String uf = "U:"+tokens[1];
			output.collect(new IntWritable(ui), new Text(uf));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}