package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advdatabase.netflix.setting.IOInfo;

public  class ColNormMatrix {
	
	public static JobConf getConfTransMatrix(String input, String output) throws IOException {
		JobConf conf = new JobConf();
		conf.setJobName("transpose matrix-row major");
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setMapperClass(TransposeMapper.class);
		conf.setReducerClass(TransposeReducer.class);
		
		//clear previous output
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(output), true);
		
		return conf;
	}
	
	public static class TransposeMapper implements Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		IntWritable movieId = new IntWritable();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
				//convert Text value to string
				  String line = value.toString();
				  //movie ratings
				  //each  <userID, MovieId, rating,date> is delimited by a line break
				  //tokenize the strings on ","
				  
				  StringTokenizer itr = new StringTokenizer(line, ",:\t");
				  try {
				      //String name to hold the movieID
				  String uid = itr.nextToken();
				  //set the movieID as the Key for the output <K V> pair
				 // int uId = Integer.parseInt(id);
				  //userId.set(uId);
				  
				  
				//get the movieId
				  while(itr.hasMoreTokens()) {
					  String mid = itr.nextToken();
					  int mId = Integer.parseInt(mid);
					  movieId.set(mId);
				  //string to hold rating and date for each movie
					  String rating = "";
				  //get the rating
					  rating = itr.nextToken();
				  //int rat = Integer.parseInt(rating);
				  //output the <movieID rating,date> to the reducer
				  
					  output.collect(movieId, convertToText(uid, rating));
				  }
			  } catch (NumberFormatException e) {
				  System.out.println("here we are-->\n"+ e.toString());
				  return;
			  } catch (IOException e) {
				  e.printStackTrace();
			  }
		}
		
		Text convertToText(String userId, String rat) {
			Text ret = new Text(userId+","+rat);
			return ret;
		}
	}
	
	public static class TransposeReducer implements Reducer<IntWritable, Text, IntWritable, Text>  {
		
		
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			List<Integer> userIds = new LinkedList<Integer>();
			List<Float> rats = new LinkedList<Float>();
			
			while(values.hasNext()) {
				String line = values.next().toString();
				StringTokenizer itr = new StringTokenizer(line, ",");
				//first is movie id
				String temp = itr.nextToken();
				try {
					int uId = Integer.parseInt(temp);
					float rat = Float.parseFloat(itr.nextToken());
					rats.add(rat);
					userIds.add(uId);
				} catch (NumberFormatException e) {
					System.out.println("catch"+e.toString());
				}
			}
			
			String movieRow = "";
			for(int i=0; i<userIds.size(); i++) {
				float normRating = rats.get(i);
				movieRow+=Integer.toString(userIds.get(i))+","+Float.toString(normRating)+":";
			}
			if(userIds.size()>0) {
				Text rowValue = new Text(movieRow.substring(0, movieRow.length()-1));
				output.collect(key, rowValue);
			}
		}

	}
}
