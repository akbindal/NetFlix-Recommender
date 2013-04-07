package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
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

public class PreUVInitialization {
	
	public static class MatrixAvgMapper implements Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		IntWritable interkey = new IntWritable(1);
		/**file input is of type
		 * userid\tsum:n:movied,rating:movieid,rating:movie...
		 * userId\tsum:n:
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
				  int start = line.indexOf('\t') + 1;
				  boolean isOver=false;
				  String stSum = "";
				  String stTot = "";
				  char ch;
				  while((!isOver) && start<line.length()) {
					  if((ch=line.charAt(start))!=':') {
						  stSum = stSum+ch;
					  } else {
						  isOver=true;
					  }
					  start++;
				  }
				  
				
				  isOver=false;
				  try {
				  while((!isOver)&& start<line.length()) {
					  if((ch=line.charAt(start))!=':') {
						  stTot = stTot+ch;
					  } else {
						  isOver=true;
					  }
					  start++;
				  }
				  } catch(StringIndexOutOfBoundsException e) {
					  System.out.println(line);
				  }
				  output.collect(interkey, convertToText(stSum, stTot));
		}
		
		static Text convertToText(String x, String y) {
			Text ret = new Text(x+","+y);
			return ret;
		}
	}
	
	public static class MatrixAvgReducer implements Reducer<IntWritable, Text, IntWritable, DoubleWritable>  {
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
				OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
				throws IOException {
			float totSum = 0;
			int totEntry = 0;
			while(values.hasNext()) {
				String line = values.next().toString();
				StringTokenizer itr = new StringTokenizer(line, ",");
				
				//sumrating,totalRating
				try {
					float rat = Float.parseFloat(itr.nextToken());
					int ratSize = Integer.parseInt(itr.nextToken());
					totSum+=rat;
					totEntry += ratSize;
				} catch (NumberFormatException e) {
					System.out.println("catch"+e.toString());
				}
			}
			float avgRat = totSum/totEntry;
			Double initialElement = Math.sqrt(avgRat/Constants.D);
			output.collect(new IntWritable(1), new DoubleWritable(initialElement));
		}

	}

	public static JobConf getConfTransMatrix(Configuration con, Class cla, String input, String output) throws IOException {
		JobConf conf = new JobConf(con, cla);
		conf.setJobName("PreUV");
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setMapperClass(MatrixAvgMapper.class);
		conf.setReducerClass(MatrixAvgReducer.class);
		conf.setNumMapTasks(250);
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));
		
		//delete previous path if any
//		FileSystem fs = FileSystem.get(conf);
//		fs.delete(new Path(output), true);
		
		return conf;
	}
}
