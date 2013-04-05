package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advdatabase.netflix.setting.IOInfo;

public class NormMatrixMapper implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	private long[] userAvg = new long[480189];
	private long[] movieAvg = new long[17770];
	
	IntWritable movieId;
	@Override
	public void configure(JobConf conf) {
		
		 try {
			 FileSystem hdfs = FileSystem.get(conf);
			 FileUtil.copyMerge(hdfs, new Path(IOInfo.TEMP_MOVIE_OUTPUT), hdfs, new Path(IOInfo.TEMP_MOVIE_MERGE)
			 						, false, conf, null);
			 FileUtil.copyMerge(hdfs, new Path(IOInfo.TEMP_USER_OUTPUT), hdfs, new Path(IOInfo.TEMP_USER_MERGE)
			 						, false, conf, null);
		      String movieCacheName = new Path(IOInfo.TEMP_MOVIE_MERGE).getName();
		      String userCacheName = new Path(IOInfo.TEMP_USER_MERGE).getName();
		      Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		      int count = 0;
		      if (null != cacheFiles && cacheFiles.length > 0) {
		        for (Path cachePath : cacheFiles) {
		        	if(cachePath.getName().equals(userCacheName)) {
		        		loadUserStat(cachePath);
		        		count++;
		        	} else if (cachePath.getName().equals(movieCacheName)) {
			            loadMovieStat(cachePath);
			            count++;
		        	}
		        	
		        	if(count>2) break;
		        }
		      }
		    } catch (IOException ioe) {
		      System.err.println("IOException reading from distributed cache");
		      System.err.println(ioe.toString());
		    }
		
	}
	void loadMovieStat(Path cachePath) throws IOException {
		
	    // note use of regular java.io methods here - this is a local file now
	    BufferedReader fileReader = new BufferedReader(
	        new FileReader(cachePath.toString()));
	    try {
	      String line;
	      while ((line = fileReader.readLine()) != null) {
	    	  StringTokenizer itr = new StringTokenizer(line, "\t");
	    	  int movieId = Integer.parseInt(itr.nextToken());
	    	  long movieAvg = Long.parseLong(itr.nextToken());
	    	  this.movieAvg[movieId]= (long) (movieAvg/2.0);
	      }
	    } finally {
	      fileReader.close();
	    }
	}
	
	void loadUserStat(Path cachePath) throws IOException {
		
	    // note use of regular java.io methods here - this is a local file now
	    BufferedReader fileReader = new BufferedReader(
	        new FileReader(cachePath.toString()));
	    try {
	      String line;
	      while ((line = fileReader.readLine()) != null) {
	    	  StringTokenizer itr = new StringTokenizer(line, "\t");
	    	  int userId = Integer.parseInt(itr.nextToken());
	    	  long userAvg = Long.parseLong(itr.nextToken());
	    	  this.userAvg[userId]= (long) (userAvg/2.0);
	      }
	    } finally {
	      fileReader.close();
	    }
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {
		//convert Text value to string
	      String line = value.toString();
	      //movie ratings are in the form "movieID,userID,rating,date"
	      //each seperate <movieID,userID,rating,date> is delimited by a line break
	      //tokenize the strings on ","
	      StringTokenizer itr = new StringTokenizer(line, "\t");
	      //skip the userid
	      itr.nextToken();
	      try {
		      //String name to hold the movieID
		      String id = itr.nextToken();
		      //set the movieID as the Key for the output <K V> pair
		      int mId = Integer.parseInt(id);
		      movieId.set(mId);
		      
		      
		      //string to hold rating and date for each movie
		      String rating = "";
		      //get the rating
		      rating = itr.nextToken();
		      int rat = Integer.parseInt(rating);
		      //output the <movieID rating,date> to the reducer
		      output.collect(movieId, new IntWritable(rat));
	      } catch (NumberFormatException e) {
	    	  System.out.println("here we are-->\n"+ e.toString());
	    	  return;
	      } catch (IOException e) {
	    	  e.printStackTrace();
	      }
	}

}
