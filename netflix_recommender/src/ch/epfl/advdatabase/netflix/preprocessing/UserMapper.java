package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UserMapper implements Mapper<LongWritable, Text, IntWritable, IntWritable>{

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
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {
			//convert Text value to string
			  String line = value.toString();
			  //movie ratings are in the form "movieID,userID,rating,date"
			  //each  <userID, MovieId, rating,date> is delimited by a line break
			  //tokenize the strings on ","
			  
			  StringTokenizer itr = new StringTokenizer(line, ",");
			  try {
			      //String name to hold the movieID
			  String id = itr.nextToken();
			  //set the movieID as the Key for the output <K V> pair
			  int mId = Integer.parseInt(id);
			  userId.set(mId);
			  
			//skip the movieId
			  itr.nextToken();
			  
			  //string to hold rating and date for each movie
			  String rating = "";
			  //get the rating
			  rating = itr.nextToken();
			  int rat = Integer.parseInt(rating);
			  //output the <movieID rating,date> to the reducer
			  output.collect(userId, new IntWritable(rat));
		  } catch (NumberFormatException e) {
			  System.out.println("here we are-->\n"+ e.toString());
			  return;
		  } catch (IOException e) {
			  e.printStackTrace();
		  }
	}

}
