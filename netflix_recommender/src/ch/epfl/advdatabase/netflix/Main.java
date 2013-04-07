package ch.epfl.advdatabase.netflix;


import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.epfl.advdatabase.netflix.preprocessing.ColNormMatrix;
import ch.epfl.advdatabase.netflix.preprocessing.RowNormMatrix;
import ch.epfl.advdatabase.netflix.preprocessing.UInitialize;
import ch.epfl.advdatabase.netflix.preprocessing.VInitialize;
import ch.epfl.advdatabase.netflix.setting.IOInfo;
import ch.epfl.advdatabase.netflix.uviteration.JobUpdateU;
import ch.epfl.advdatabase.netflix.uviteration.JobUpdateV;
import ch.epfl.advdatabase.netflix.uviteration.JoinUM;

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
		
		/***read input and create normalized matrix-row major****/
		JobConf confRow = RowNormMatrix.getConfRNormMatrix(getConf(), getClass(), 
				args[0], IOInfo.CACHE_ROW_MATRIX);
		Job job1 = new Job(confRow);
		jc.addJob(job1);
//		//merge outputfile to somewhere
//		FileSystem hdfs = FileSystem.get(confRow);
//		FileUtil.copyMerge(hdfs, new Path(IOInfo.TEMP_USER_OUTPUT), hdfs, new Path(IOInfo.CACHE_ROW_MATRIX)
//		 						, false, confRow, null);
		/****create column major matrix representation****/
		JobConf confCol = ColNormMatrix.getConfTransMatrix(getConf(), getClass(), 
				IOInfo.CACHE_ROW_MATRIX, IOInfo.CACHE_COL_MATRIX);
		Job job2 = new Job(confCol);

		job2.addDependingJob(job1);
		jc.addJob(job2);
		
		/***find normalized rating average, userids and movieIds***/
//		JobConf jcPreUV = PreUVInitialization.getConfTransMatrix(getConf(), getClass(), IOInfo.CACHE_ROW_MATRIX, IOInfo.CACHE_PRE_UV);
//		Job job3 = new Job(jcPreUV);
//		job3.addDependingJob(job1);
//		jc.addJob(job3);
		
		
		/**initialize uv***/
		JobConf jcInitialU = UInitialize.getJobConfig(getConf(), getClass(), 
				IOInfo.TRASH, IOInfo.OUTPUT_U_INITIALIZATION);
		Job job4 = new Job(jcInitialU);
		jc.addJob(job4);
		
		JobConf jcInitialV = VInitialize.getJobConfig(getConf(), getClass(), 
				IOInfo.TRASH, IOInfo.OUTPUT_V_INITIALIZATION);
		Job job5 = new Job(jcInitialV);
		jc.addJob(job5);
		
		//jc.run();
		Thread runjobc = new Thread(jc);
        runjobc.start();
        while( !jc.allFinished())
        {
            System.out.println("jobs left="+(jc.getWaitingJobs().size()+jc.getRunningJobs().size()));//do whatever you want; just wait or ask for job information
            Thread.sleep(5000);
        }
        JobControl jbc = new JobControl("uv update");
        /**join of um**/
//        JobConf jcJoinU = JoinUM.getJobConfig(getConf(), getClass(), IOInfo.TEMP_JOIN_UM);
//        Job job7 = new Job(jcJoinU);
//        jbc.addJob(job7);
		/***iteration of uv***/
        int iter=1;
        while(iter<20) {
			JobConf jcupdateU = JobUpdateU.getJobConfig(getConf(), getClass(), 
					IOInfo.OUTPUT_U+(iter-1), IOInfo.CACHE_ROW_MATRIX, 
					IOInfo.OUTPUT_U+iter, IOInfo.OUTPUT_V+(iter-1));
			JobClient.runJob(jcupdateU);
			//Job job6 = new Job(jcupdateU);
			//jbc.addJob(job6);
	//		job6.addDependingJob(job4);
	//		job6.addDependingJob(job1);
	//		job6.addDependingJob(job5);
	//		runjobc = new Thread(jbc);
	//		runjobc.start();
	//		while( !jbc.allFinished())
	//        {
	//            System.out.println("jobs left="+(jbc.getWaitingJobs().size()+jbc.getRunningJobs().size()));//do whatever you want; just wait or ask for job information
	//            Thread.sleep(2000);
	//        }
			//calcualteRmse();
			JobConf jcupdateV = JobUpdateV.getJobConfig(getConf(), getClass(), 
					IOInfo.CACHE_COL_MATRIX, IOInfo.OUTPUT_V+(iter-1), 
					IOInfo.OUTPUT_V+iter, IOInfo.OUTPUT_U+(iter));
			//Job job7 = new Job(jcupdateV);
			JobClient.runJob(jcupdateV);
			FileSystem fs = FileSystem.get(jcupdateV);
			fs.rename(new Path(IOInfo.OUTPUT_V+iter+"/rmse-r-00000"), new Path("/std57/rmse"));
			//JobConf jcrmse = RMSE.getJobConfi(getConf(), getClass());
			float rmse = readRmse("/std57/rmse");
			System.out.println(iter+":"+rmse);
			iter++;
        }	
			//job7.addDependingJob(job6);
		//jbc.addJob(job7);
		//jbc.run();
//		int iter=0;
//		while(true) {
//			//row range and column range
//			
//		}
		
		
		//JobClient.runJob(confRow);
	    return 0;
	}
	
	public float readRmse(String path) {
		BufferedReader fileReader=null;
		float rmse=0;
		try {
			fileReader= new BufferedReader(
			        new FileReader(path.toString()));
			String line;

			while ((line = fileReader.readLine()) != null) {
				String[] tokens = line.split(":", 2);
				if(!tokens[1].equals("NaN")) {
					rmse += Float.parseFloat(tokens[1]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
		      try {
				fileReader.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
		return rmse;
	}
}
