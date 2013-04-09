package ch.epfl.advdatabase.netflix.uviteration;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class UMReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>  {
	
	float[] rat = new float[17770];
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
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 * input1:<uid, <'M':movieId,Norm_rat1:movieId2,Norm_rat2:...>>
	 * input2:<uid, <'U':uf1,uf2...>>
	 * output:<userid:movieId:uf1,uf2,uf3:rating>*> 
	 */
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		String movieRatPairs="";
		String ufeature="";
		//we will have only two values
		while(values.hasNext()) {
			String line = values.next().toString();
			String[] tokens = line.split(":", 2);
			//first is movie id
			String tupleType = tokens[0];
			if(tupleType.equals("M")) {
				movieRatPairs = tokens[1];
			} else if(tupleType.equals("U")){
				ufeature= tokens[1];
			}
		}
		String outputValue = key.toString() +":"+ ufeature +":";
		
		//parse movieId from movieRatinPairs
		StringTokenizer itr = new StringTokenizer(movieRatPairs,":");
		while(itr.hasMoreTokens()) {
			String tempToken[] = itr.nextToken().split(",", 2);
			String movieId = tempToken[0];
			int mid = Integer.parseInt(movieId);
			String rating = tempToken[1];
			Text value = new Text (outputValue+rating);
			output.collect(new IntWritable(mid), value);
		}
	}
}