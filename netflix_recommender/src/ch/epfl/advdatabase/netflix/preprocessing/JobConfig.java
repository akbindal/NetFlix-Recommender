package ch.epfl.advdatabase.netflix.preprocessing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;

import ch.epfl.advdatabase.netflix.preprocessing.RowNormMatrix.UserRowMapper;
import ch.epfl.advdatabase.netflix.preprocessing.RowNormMatrix.UserRowReducer;
import ch.epfl.advdatabase.netflix.setting.IOInfo;



public class JobConfig {
	
	public static JobConf getMovieStat(String input) throws IOException {
		// configuration should contain reference to your namenode
		JobConf conf = new JobConf();
		
		//job.setJarByClass(LastEdit.class);
		conf.setJobName("Movie Id Statisitic");
		//conf.setJarByClass()
		
		//job.setCombinerClass(MovieMapper.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(MovieMapper.class);
		conf.setReducerClass(MovieReducer.class);
		//conf.setNumMapTasks(2);
		//conf.setNumReduceTasks(5);
		
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(IOInfo.TEMP_MOVIE_OUTPUT));
		
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(IOInfo.TEMP_MOVIE_OUTPUT), true);
		
		return conf;
	}
	
	public static JobConf getUserStat(String input) throws IOException {
		JobConf conf = new JobConf();		
		
		conf.setJobName("calculate User Stat");
		
//		try {
//			FileSystem hdfs = FileSystem.get(conf);
//			FileUtil.copyMerge(hdfs, new Path("temp/user"), hdfs, new Path(dstPath), false, conf, null);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		//job.setCombinerClass(MovieMapper.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setMapperClass(UserMapper.class);
		conf.setReducerClass(UserReducer.class);
		
		FileInputFormat.addInputPath(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(IOInfo.TEMP_USER_OUTPUT));
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(IOInfo.TEMP_USER_OUTPUT), true);
		return conf;
	}
	
	public static JobConf getNormMatrix(String inData, String inUserStat, String inMovieStat) throws IOException {
		
		JobConf conf = new JobConf();
		conf.setJobName("create normalized matrix");
		

		
		//job.setCombinerClass(MovieMapper.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(LongWritable.class);
		DistributedCache.addCacheFile(new Path(IOInfo.TEMP_USER_MERGE).toUri(), conf);
		DistributedCache.addCacheFile(new Path(IOInfo.TEMP_MOVIE_MERGE).toUri(), conf);
		
		conf.setMapperClass(UserMapper.class);
		conf.setReducerClass(UserReducer.class);
		
		FileInputFormat.addInputPath(conf, new Path(inData));
		FileOutputFormat.setOutputPath(conf, new Path(IOInfo.TEMP_USER_OUTPUT));
		
		FileSystem fs = FileSystem.get(new Configuration());
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(IOInfo.TEMP_USER_OUTPUT), true);
		
		return conf;
	}
	
}
