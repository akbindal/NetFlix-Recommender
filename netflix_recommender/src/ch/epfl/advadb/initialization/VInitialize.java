package ch.epfl.advadb.initialization;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advadb.setting.Constants;

public class VInitialize {
	public static class VInitiliazeMapper implements Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			for(int i=0; i< Constants.V_FILES; i++) {
				output.collect(new IntWritable(i), new Text(""));
			}
		}
	}
	
	public static class VInitiliazeReducer implements Reducer<IntWritable, Text, Text, Text>  {
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		
		Text outputvalue = new Text();
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			int k = key.get();
			int currentMovie = k*Constants.V_SPLIT_SIZE+1;
			int endFile = (k+1)*Constants.V_SPLIT_SIZE;
			
			
			String value="";

			Random generator = new Random();
			double number;
			int avg=0;
			double deviation=0.2;
			
			Text outputKey;
			while(currentMovie <= endFile && currentMovie <= Constants.NO_MOVIES) {
				
				for(int i =1 ;i <= Constants.D; i++) {
					outputKey = new Text("V,"+i);
					number = avg+generator.nextDouble() *deviation - deviation/2;
					value=currentMovie+","+Double.toString(number);
					outputvalue.set(value);
					output.collect(outputKey,outputvalue );
				}
				currentMovie++;
			}
		}

	}
	
	
	public static JobConf getJobConfig(Configuration con, Class cla, String input, String output) throws IOException {
		JobConf conf = new JobConf(con, cla);
		conf.setJobName("V Initialization");
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setMapperClass(VInitiliazeMapper.class);
		conf.setReducerClass(VInitiliazeReducer.class);
		conf.setNumReduceTasks(Constants.V_FILES);
		conf.set("mapred.textoutputformat.separator",",");
		
		//create empty file
		FileSystem fs = FileSystem.get(conf);
		OutputStream os = fs.create(new Path(input));
		os.write(1); os.close();
		fs.delete(new Path(output), true);
		
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		return conf;
	}
}

