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

import ch.epfl.advdatabase.netflix.preprocessing.ColNormMatrix;
import ch.epfl.advdatabase.netflix.preprocessing.ColNormMatrix.TransposeMapper;
import ch.epfl.advdatabase.netflix.preprocessing.ColNormMatrix.TransposeReducer;
import ch.epfl.advdatabase.netflix.preprocessing.RowNormMatrix;
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
		
		JobControl jc = new JobControl("create normalized matrix");
		
		//read input and create normalized matrix-row major
		JobConf confRow = RowNormMatrix.getConfRNormMatrix(getConf(), getClass(), args[0], IOInfo.CACHE_ROW_MATRIX);
		Job job1 = new Job(confRow);
		jc.addJob(job1);
		
		//create column major matrix representation
		JobConf confCol = ColNormMatrix.getConfTransMatrix(getConf(), getClass(), IOInfo.CACHE_ROW_MATRIX, IOInfo.CACHE_COL_MATRIX);
		Job job2 = new Job(confCol);

		job2.addDependingJob(job1);
		jc.addJob(job2);
		jc.run();
//		Thread runjobc = new Thread(jc);
//        runjobc.start();
//        while( !jc.allFinished())
//        {
//            System.out.println("jobs left="+jc.getWaitingJobs().size());//do whatever you want; just wait or ask for job information
//            Thread.sleep(15000);
//        }
		//JobClient.runJob(confRow);
	    return 0;
	}
}
