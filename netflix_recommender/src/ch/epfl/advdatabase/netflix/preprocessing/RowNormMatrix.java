package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

/*
 * read dataset and create normalized matrix 
 * (Horizontally Normalized: user ratings are Normalized) - row major
 * It takes the input from the "arg[0]" and throw the output in "/std57/cache/matrix/row"
 * 
 */
public  class RowNormMatrix  {
	
	/*
	 * gives the configuration of the job: UserMapper, UserReducer
	 */
	public static JobConf getJobConfig(Configuration con, Class cla, String input, String output) throws IOException {
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
		
		//Delete the output.
		fs.delete(new Path(output), true);
		fs.delete(new Path(IOInfo.CACHE_COL_MATRIX), true);
		return conf;
	}
	
	/*
	 * 
	 */
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
		
		/*
		 * Input: <userId, movieId, rating, date>
		 * output<Key, <Value>>: <userId, <movieId,rating>>
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
				//convert Text value to string
			String line = value.toString();
			
				//each  <userID, MovieId, rating,date> is delimited by a ","
				//tokenize the strings on ","
			String[] tokens = line.split(",");
			try {
				String uid = tokens[0];//userId
				int uId = Integer.parseInt(uid);
				userId.set(uId);
				  
					//get the movieId
				String mid = tokens[1];
				  
				   	//get the rating
				String rating = tokens[2];
				
				output.collect(userId, convertToText(mid,rating));
				
			  } catch (NumberFormatException e) {
				  System.out.println("BadRecord\n"+ e.toString());
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
	
	/*
	 * Input<Key, <Value>*>: <userId, <movieId,Norm_rating>*>
	 * output<userId:movieId1,Norm_rating1:movieId2,Norm_rating2:movieId3,Norm_rating3:...>
	 * (* :: list)
	 */
	public static class UserRowReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>  {
		
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
			float sumRating = 0;
			int totRating =0;
			List<Integer> movieIds = new ArrayList<Integer>();
			List<Integer> rats = new ArrayList<Integer>();
			while(values.hasNext()) { 
				String line = values.next().toString();
				String[] movieRatPair = line.split(",");
				
				//first is movie id
				int mId = Integer.parseInt(movieRatPair[0]);
				int rating = Integer.parseInt( movieRatPair[1]);
				movieIds.add(mId);
				rats.add(rating);
				
				sumRating += rating;   
				totRating++;
			}
			
			//taking  average of the users
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
