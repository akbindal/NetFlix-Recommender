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
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public  class ColNormMatrix {
	
	public static JobConf getConfTransMatrix(Configuration con, Class cla, String input, String output) throws IOException {
		JobConf conf = new JobConf(con, cla);
		conf.setJobName("transpose matrix-row major");
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.set("mapred.textoutputformat.separator",":");
		conf.setMapperClass(TransposeMapper.class);
		conf.setReducerClass(TransposeReducer.class);
		conf.setNumMapTasks(150);
		conf.setNumReduceTasks(150);

		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		FileSystem fs = FileSystem.get(conf);
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
		/**file input is of type
		 * userid\tsum:n:movied,rating:movieid,rating:movie...
		 * userId\tsum:n:
		 * 3:1,-0.5:5,0.5
		 */
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
				//convert Text value to string
				  String line = value.toString();
				  //movie ratings
				  //each  <userID, MovieId, rating,date> is delimited by a line break
				  //tokenize the strings on ","
				  String[] tokens = line.split(":");
				 // StringTokenizer itr = new StringTokenizer(line, ",:\t");
				  try {
				      //String name to hold the movieID
				  String uid =  tokens[0];//itr.nextToken();
				  //set the movieID as the Key for the output <K V> pair
				 // int uId = Integer.parseInt(id);
				  //userId.set(uId);
				  
				  //itr.nextToken();itr.nextToken(); //skip initial two tokens
				  
				//get the movieId
				  for(int i =1; i<tokens.length; i++) {
					  String[] ratingPair =  tokens[i].split(",");
					  int mid = Integer.parseInt(ratingPair[0]);
					  movieId.set(mid);
					  String rating = ratingPair[1];
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
			
			//List<String> userIds = new ArrayList<String>();
			//List<String> rats = new ArrayList<String>();
			
			String userRow = "";
			while(values.hasNext()) {
				String line = values.next().toString();
				String[] tokens = line.split(",");//new StringTokenizer(line, ",");
				//first is movie id
				
				try {
				//	int uId = Integer.parseInt(tokens[0]);
				//	float rat = Float.parseFloat(tokens[1]);
				//	rats.add(tokens[1]);
				//	userIds.add(tokens[0]);
					userRow+=tokens[0]+","+tokens[1]+":";
				} catch (NumberFormatException e) {
					System.out.println("catch"+e.toString());
				}
			}
			
			if(userRow.length()> 0) {
				Text colValue = new Text(userRow);
				output.collect(key, colValue);
			}
		}

	}
}
