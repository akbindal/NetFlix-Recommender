package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advdatabase.netflix.setting.Constants;
import ch.epfl.advdatabase.netflix.setting.IOInfo;

public  class RowNormMatrix  {
	public static JobConf getConfRNormMatrix(Configuration con, Class cla, String input, String output) throws IOException {
		JobConf conf = new JobConf(con, cla);
		conf.setJobName("create normalized matrix-row major");
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(UserRowMapper.class);
		conf.setReducerClass(UserRowReducer.class);
		conf.setNumMapTasks(Constants.U_FILES);
		conf.setNumReduceTasks(Constants.U_FILES);
		
		conf.set("mapred.textoutputformat.separator",":");
		
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		FileSystem fs = FileSystem.get(conf);
		//Delete everyoutput, this should be in the driver but let it go.
		fs.delete(new Path(output), true);
		fs.delete(new Path(IOInfo.CACHE_COL_MATRIX), true);
		return conf;
	}
	
	public static class UserRowMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		IntWritable userId = new IntWritable();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
				//convert Text value to string
				  String line = value.toString();
				  //movie ratings
				  //each  <userID, MovieId, rating,date> is delimited by a line break
				  //tokenize the strings on ","
				  String[] tokens = line.split(",");
				  //StringTokenizer itr = new StringTokenizer(line, ",");
				  try {
				      //String name to hold the movieID
				  String uid = tokens[0];//itr.nextToken();
				  //set the movieID as the Key for the output <K V> pair
				  int uId = Integer.parseInt(uid);
				  userId.set(uId);
				  
				//get the movieId
				  String mid = tokens[1];//itr.nextToken();
				  
				  String rating;
				  //get the rating
				  rating = tokens[2];//itr.nextToken();
				  //int rat = Integer.parseInt(rating);
				  //output the <movieID rating,date> to the reducer
				  
				  output.collect(userId, convertToText(mid, rating));
			  } catch (NumberFormatException e) {
				  System.out.println("here we are-->\n"+ e.toString());
				  return;
			  } catch (IOException e) {
				  e.printStackTrace();
			  }
		}
		
		Text convertToText(String movieId, String rat) {
			Text ret = new Text(movieId+","+rat);
			return ret;
		}
	}
	
	public static class UserRowReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>  {
		
		float[] rat = new float[17770];
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		//input=<uid:<movieid:rat>*> output=<uid:movieid1,Normalized_rat1:movieid2,Normalized_rat2:....>
		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			float sumRating = 0;
			int totRating =0;
			List<Integer> movieIds = new ArrayList<Integer>();
			List<Integer> rats = new ArrayList<Integer>();
			while(values.hasNext()) { //"movieId,rat"
				String line = values.next().toString();
				String[] tokens = line.split(",");
				
				//first is movie id
				String temp = tokens[0];
				int mId = Integer.parseInt(temp);
				int rating = Integer.parseInt( tokens[1]);
				rats.add(rating);
				movieIds.add(mId);
				sumRating += rating;   
				totRating++;
			}
			float avg = ((float)sumRating)/(float)totRating;
			String userRow = "";
			for(int i=0;i<movieIds.size();i++) {
				float normRating = rats.get(i)-avg;
				userRow+=Integer.toString(movieIds.get(i))+","+Float.toString(normRating)+":";
			}
			Text rowValue = new Text(userRow.substring(0, userRow.length()-1));
			output.collect(key, rowValue);
		}
	}
	
	
}
