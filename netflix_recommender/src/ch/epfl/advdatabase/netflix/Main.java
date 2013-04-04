package ch.epfl.advdatabase.netflix;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.epfl.advdatabase.netflix.preprocessing.ColNormMatrix.TransposeMapper;
import ch.epfl.advdatabase.netflix.preprocessing.ColNormMatrix.TransposeReducer;
import ch.epfl.advdatabase.netflix.preprocessing.RowNormMatrix.UserRowMapper;
import ch.epfl.advdatabase.netflix.preprocessing.RowNormMatrix.UserRowReducer;
import ch.epfl.advdatabase.netflix.setting.IOInfo;

public class Main extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		if (args.length < 0) {
			System.err.println("Usage: ix.lab01.wikipedia.LastEdit <input path> <output path>");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		 
		System.exit(res);
//		JobConf confRowMatrix = RowNormMatrix.getConfRNormMatrix(args[0], IOInfo.CACHE_ROW_MATRIX);
//		//JobConf confMovieStat = JobConfig.getMovieStat(args[0]);
//		JobConf confColMatrix = ColNormMatrix.getConfTransMatrix(IOInfo.CACHE_ROW_MATRIX, IOInfo.CACHE_COL_MATRIX);
//		
//		//Job jobMovieStat = new Job(confMovieStat);
//		Job jobRowNormMatrix = new Job(confRowMatrix);
//		Job jobColNormMatrix = new Job(confColMatrix);
//		jobColNormMatrix.addDependingJob(jobRowNormMatrix);
//		JobControl jc = new JobControl("create norm matrix");
//		jc.addJob(jobRowNormMatrix);
//		jc.addJob(jobColNormMatrix);
		
		//jc.addJob(jobMovieStat);	
		//matrix normalization
		//calculate 
//		JobConf confMovieStat = JobConfig.getMovieStat(args[0]);
//		JobConf confUserStat = JobConfig.getUserStat(args[0]);
//		//JobConf confNormMatrix = JobConfig.getNormMatrix();
//		Job jobMovieStat = new Job(confMovieStat);
//		Job jobUserStat = new Job(confUserStat);
//		//Job jobNormMatrix = new Job(confNormMatrix);
//		//jobNormMatrix.addDependingJob(jobMovieStat);
//		//jobNormMatrix.addDependingJob(jobUserStat);
//		JobControl jc = new JobControl("create normmatrix");
//		jc.addJob(jobMovieStat);					
//		jc.addJob(jobUserStat);
		//jc.run();
		
		System.out.println("next after conf");
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobControl jc = new JobControl("create normalized matrix");
		
		//read input and create normalized matrix-row major
//		JobConf confRow = RowNormMatrix.getConfRNormMatrix(args[0], IOInfo.CACHE_ROW_MATRIX);
//		JobClient.runJob(confRow);
		
		JobConf conf = new JobConf(getConf(), getClass());//(RowNormMatrix.class);
		conf.setJobName("create normalized matrix-row major");
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(UserRowMapper.class);
		conf.setReducerClass(UserRowReducer.class);
		conf.setNumMapTasks(80);
		conf.setNumReduceTasks(80);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(IOInfo.CACHE_ROW_MATRIX));
		
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(IOInfo.CACHE_ROW_MATRIX), true);
		Job job1 = new Job(conf);
		jc.addJob(job1);
		//jc.run();
		//create column major matrix representation
//		JobConf confCol = ColNormMatrix.getConfTransMatrix(IOInfo.CACHE_ROW_MATRIX, IOInfo.CACHE_COL_MATRIX);
//		JobClient.runJob(confCol);
		JobConf confCol = new JobConf(getConf(), getClass());
		confCol.setJobName("transpose matrix-row major");
		confCol.setMapOutputKeyClass(IntWritable.class);
		confCol.setMapOutputValueClass(Text.class);
		confCol.setOutputKeyClass(IntWritable.class);
		confCol.setOutputValueClass(Text.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		confCol.setMapperClass(TransposeMapper.class);
		confCol.setReducerClass(TransposeReducer.class);
		confCol.setNumMapTasks(80);
		confCol.setNumReduceTasks(80);
		//clear previous output
		FileInputFormat.addInputPath(confCol, new Path(IOInfo.CACHE_ROW_MATRIX));
		FileOutputFormat.setOutputPath(confCol, new Path(IOInfo.CACHE_COL_MATRIX));
		
		//fs = FileSystem.get(confCol);
		fs.delete(new Path(IOInfo.CACHE_COL_MATRIX), true);
		Job job2 = new Job(confCol);
		
		job2.addDependingJob(job1);
		jc.addJob(job2);
		//jc.run();
		Thread runjobc = new Thread(jc);
        runjobc.start();
        while( !jc.allFinished())
        {
            //do whatever you want; just wait or ask for job information
        }
		//JobClient.runJob(confRow);
	    return 0;
	}
}
