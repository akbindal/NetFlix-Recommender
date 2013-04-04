package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UserReducer implements Reducer<IntWritable, IntWritable, IntWritable, LongWritable>  {
	private LongWritable avgRating = new LongWritable();
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(IntWritable key, Iterator<IntWritable> values,
			OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
			throws IOException {
		int sum = 0;
		int size =0;
		while(values.hasNext()) {
			sum += values.next().get();   
			size++;
		}
		long avg = ((long)sum)/size;
		avgRating.set(avg);
		output.collect(key, avgRating);
	}

}
