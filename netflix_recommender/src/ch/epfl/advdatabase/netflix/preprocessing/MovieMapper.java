package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

public class MovieMapper implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private IntWritable moviedId = new IntWritable();  
	//input <userid, movieid, rating, date> -> <movieid, rating>
    public void map(LongWritable key, Text valuex, 
                    OutputCollector<IntWritable, IntWritable> output, 
                    Reporter reporter) throws IOException {
    
      //convert Text value to string
      String line = valuex.toString();
      //movie ratings are in the form "movieID,userID,rating,date"
      //each seperate <movieID,userID,rating,date> is delimited by a line break
      //tokenize the strings on ","
      StringTokenizer itr = new StringTokenizer(line, ",");
      //skip the userid
      itr.nextToken();
      try {
	      //String name to hold the movieID
	      String id = itr.nextToken();
	      //set the movieID as the Key for the output <K V> pair
	      int mId = Integer.parseInt(id);
	      moviedId.set(mId);
	      
	      
	      //string to hold rating and date for each movie
	      String rating = "";
	      //get the rating
	      rating = itr.nextToken();
	      int rat = Integer.parseInt(rating);
	      //output the <movieID rating,date> to the reducer
	      output.collect(moviedId, new IntWritable(rat));
      } catch (NumberFormatException e) {
    	  System.out.println("here we are-->\n"+ e.toString());
    	  return;
      } catch (IOException e) {
    	  e.printStackTrace();
      }
    }
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
