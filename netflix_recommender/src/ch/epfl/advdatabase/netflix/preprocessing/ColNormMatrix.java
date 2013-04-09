package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;
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

import ch.epfl.advdatabase.netflix.setting.Constants;


/**
 * It defines the map and reduce inner classes for the Transpose job 
 * The task of the transpose-job is to read the row-major matrix and return 
 * column Major matrix (transpose of row-major)
 * the output from this transpose-job will be used 
 * in {@ch.epfl.advadb.netflix.JobUpdateV}jobUpdateV to update the
 * Feature vectors of V
 * @author ashish
 *
 */
/**
 * @author ashish
 *
 */
public  class ColNormMatrix {
	
	/**
	 * 
	 * @param con: Configuration instance from class which is implementing ToolRunner
	 * @param cla: Class instance of class which is implementing ToolRunner
	 * @param input: to set the Input path for the the Map task <userId:movieId1,rat1:movieId2,rat2:...>
	 * @param output: to set the Output path for the Reduce task <movieId:userId1,rat1:movieId2,rat2:..>
	 * @return JobConf which has the map:TransposeMapper and reduce:TransposeReducer
	 * @throws IOException if input files are not accessible. 
	 */
	public static JobConf getJobConfig(Configuration con, Class cla, String input, String output) throws IOException {
		
		JobConf conf = new JobConf(con, cla);
		conf.setJobName("Transpose(row major)=col-major");
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.set("mapred.textoutputformat.separator",":");
		conf.setMapperClass(TransposeMapper.class);
		conf.setReducerClass(TransposeReducer.class);
		
		conf.setNumMapTasks(Constants.U_FILES);
		conf.setNumReduceTasks(Constants.V_FILES);

		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);
		
		return conf;
	}
	
	/**
	 * Inner class of ColNormMatrix is responsible for Map task
	 * input: <userId:movieId1,rat1:movieId2,rat2:...>
	 * output: <movieId, <userId,rat>*>
	 * @author ashish
	 *
	 */
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
				  
				//user id is delimited by ":"
				//each  <userId:movieId1,rat1:movieId2,rat2:...> 
			String[] tokens = line.split(":");
			
			try {
				String uid =  tokens[0];//itr.nextToken();
			    //get the array of movie rating pair
				for(int i =1; i<tokens.length; i++) {
					String[] ratingPair =  tokens[i].split(",");
					int mid = Integer.parseInt(ratingPair[0]);
					movieId.set(mid);
					String rating = ratingPair[1];
					output.collect(movieId, convertToText(uid, rating));
				}  
			  } catch (NumberFormatException e) {
				 System.out.println("BadRecord\n"+ e.toString());
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
	
	
	/**
	 * Inner class of ColNormMatrix is responsible for Reduce task
	 * input: <userId:movieId1,rat1:movieId2,rat2:...>
	 * input: <movieId, <userId,rat>*>
	 * output: <movieId:userId1,rat1:userId2,rat2:userId3,rat3...>
	 * @author ashish
	 *
	 */
	public static class TransposeReducer implements Reducer<IntWritable, Text, IntWritable, Text>  {
		@Override
		public void configure(JobConf job) {
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			//user row : concatenation of all the userid,rat pair with separator":"
			String userRow = "";
			while(values.hasNext()) {
				String line = values.next().toString();
				String[] tokens = line.split(",");
				
				try {
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
