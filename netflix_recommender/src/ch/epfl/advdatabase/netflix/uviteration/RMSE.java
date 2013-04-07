package ch.epfl.advdatabase.netflix.uviteration;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

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

import ch.epfl.advdatabase.netflix.preprocessing.UInitialize.UInitiliazeMapper;
import ch.epfl.advdatabase.netflix.preprocessing.UInitialize.UInitiliazeReducer;
import ch.epfl.advdatabase.netflix.setting.Constants;

public class RMSE {
	public static class UInitiliazeMapper implements Mapper<LongWritable, Text, IntWritable, Text>{

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
			
			for(int i=0; i< Constants.U_FILES; i++) {
				output.collect(new IntWritable(i), new Text());
			}
		}
	}
	
	public static class UInitiliazeReducer implements Reducer<IntWritable, Text, IntWritable, Text>  {
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
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			int k = key.get();
			int currentUser = k*Constants.U_SPLIT_SIZE+1;
			int endFile = (k+1)*Constants.U_SPLIT_SIZE;

			String value="";
			for(int i =0 ;i < Constants.D-1; i++) {
				value+="1,";
			}
			value += "1";
			
			outputvalue.set(value);
			
			while(currentUser <= endFile && currentUser <= Constants.NO_USER) {
				output.collect(new IntWritable(currentUser),outputvalue );
				currentUser++;
			}
		}

	}
	
	
	public static JobConf getJobConfig(Configuration con, Class cla, String input, String output) throws IOException {
		JobConf conf = new JobConf(con, cla);
		conf.setJobName("U Initialization");
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setMapperClass(UInitiliazeMapper.class);
		conf.setReducerClass(UInitiliazeReducer.class);
		conf.setNumReduceTasks(Constants.U_FILES);
		conf.set("mapred.textoutputformat.separator",":");
		
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
