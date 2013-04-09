package ch.epfl.advdatabase.netflix.uviteration;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class VReadMapper implements Mapper<LongWritable, Text, IntWritable, Text>{
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
	 * input <vid:vf1,vf2>
	 * output <vid, <'V':vf1,vf2...>>
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String[] tokens = line.split(":",2);
		//StringTokenizer itr = new StringTokenizer(line, ":");
		try { 
			String vid = tokens[0];//titr.nextToken();
			int vi = Integer.parseInt(vid);
			String vf = "V:"+tokens[1];//itr.nextToken();
			if(vf.length()<25) {
				System.out.println("kjlk");
			}
			output.collect(new IntWritable(vi), new Text(vf));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
