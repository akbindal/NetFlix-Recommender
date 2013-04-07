package ch.epfl.advdatabase.netflix.uviteration;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Uupdate {
	//mapper:Map(<ui,vj,uf,vf,rat>) => (ui, <uf, p, v2>)
	//reducer Reduce(ui, <uf, p, v2>*) => <’U’,ui,uf’>
	//load v in distributed cache
	
	public static class UUpdateMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		public void configure(JobConf job) {
			//load distributed cache of V
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
		}
		
		IntWritable userId = new IntWritable();
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 * input=<ui:vf:uf1,uf2,uf3:rat> output=> <(ui,vj)
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
				  
				  StringTokenizer itr = new StringTokenizer(line, ",");
				  try {
				      //String name to hold the movieID
				  String uid = itr.nextToken();
				  //set the movieID as the Key for the output <K V> pair
				  int uId = Integer.parseInt(uid);
				  userId.set(uId);
				  if(uId==3) {
					  uId+=0;
				  }
				  
				//get the movieId
				  String mid = itr.nextToken();
				  
				  String rating;
				  //get the rating
				  rating = itr.nextToken();
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
}
