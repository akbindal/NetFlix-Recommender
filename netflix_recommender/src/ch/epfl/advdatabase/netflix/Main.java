package ch.epfl.advdatabase.netflix;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.epfl.advdatabase.netflix.preprocessing.ColNormMatrix;
import ch.epfl.advdatabase.netflix.preprocessing.RowNormMatrix;
import ch.epfl.advdatabase.netflix.setting.IOInfo;

public class Main extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		if (args.length < 0) {
			System.err.println("Usage: ix.lab01.wikipedia.LastEdit <input path> <output path>");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		System.exit(res);
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
