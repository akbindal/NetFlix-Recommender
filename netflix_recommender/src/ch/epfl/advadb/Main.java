package ch.epfl.advadb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

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

import ch.epfl.advadb.initialization.UInitialize;
import ch.epfl.advadb.initialization.VInitialize;
import ch.epfl.advadb.preprocessing.ColNormMatrix;
import ch.epfl.advadb.preprocessing.NormColMatrix;
import ch.epfl.advadb.preprocessing.RowNormMatrix;
import ch.epfl.advadb.setting.Constants;
import ch.epfl.advadb.setting.IOInfo;
import ch.epfl.advadb.uviteration.JobUpdateU;
import ch.epfl.advadb.uviteration.JobUpdateV;

public class Main extends Configured implements Tool {
	
	public static boolean CALCULATE_RMSE=false;
	
	public static boolean BIG_DATSET=false;
	
	public static boolean ITERATION=false;
	
	public final static int NO_NODES = 88;

	
	public static void main(String[] args) throws Exception {
		if (args.length < 0) {
			System.err
					.println("Usage: ch.epfl.advb.Main <input path> <output path>");
			System.exit(-1);
		}
		setup(BIG_DATSET, NO_NODES, args[1]);
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		System.exit(res);
	}

	/**
	 * This is responsible to setup the constants (netflix.settings.constants) which are responsible 
	 * for setting up the no of mappers and no of reducers.
	 * No of mappers depends on no of input files therefore U and V are splitted in a way such that
	 * no of files are generated according to no of mappers.
	 * see constant U_SPLIT_SIZE
	 * which are used by 
	 * @param bigDataset: sets up the no of user and movies according to the type of dataset:small, big
	 * @param no_nodes: no of cluster nodes which are accessible for Reduce
	 */
	public static void setup(boolean bigDataset, int no_nodes, String output) {
		if(bigDataset) {
			Constants.NO_MOVIES=17770;
			Constants.NO_USER=480189;
			IOInfo.CACHE_ROW_MATRIX="/std57/bigcache/matrix/row";
			IOInfo.CACHE_COL_MATRIX="/std57/bigcache/matrix/col";
		} else {
			Constants.NO_MOVIES=99;
			Constants.NO_USER=5000;
			IOInfo.CACHE_ROW_MATRIX="/std57/smallcache/matrix/row";
			IOInfo.CACHE_COL_MATRIX="/std57/smallcache/matrix/col";
		}
		//Constants.U_FILES = 0.95*no_nodes*;
		//System.out.println("max="+ );
		int maxTask=2;
		//new JobConf().getInt("mapred.tasktracker.reduce.tasks.maximum", maxTask);
		int NO_of_REDUCER = no_nodes; //(int) (no_nodes*maxTask*0.95);
		Constants.V_FILES = NO_of_REDUCER;
		Constants.U_FILES = NO_of_REDUCER;

		IOInfo.OUTPUT_U_INITIALIZATION = output+"/U_0";
		IOInfo.OUTPUT_V_INITIALIZATION = output+"/V_0";
		
		IOInfo.OUTPUT_V = output+"/V_";
		IOInfo.OUTPUT_U = output+"/U_";
	}
	
	/*
	 * All the job sequences for the UV decomposition algorithm
	 * is written. 
	 * In order to run the jobs, This function use two methods which are defined in mapred api
	 * 1. JobClient runs the single job.
	 * 2. JobControl best to run the sequence of dependent jobs in easier way, but this is bit 
	 * slower than JobClient for running sequential job (slower in terms of setting up the jobs)
	 * 
	 * Therefore for the preprocessing, initialization, JobControl is used
	 * but for iteration, Jobs are executed through JobClient. 
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		if(!ITERATION) {
		JobControl jc = new JobControl("Matrix Normalization");
		
		/**** Take the traspose of Row Major => Column Major ****/
		JobConf confCol = NormColMatrix.getJobConfig(getConf(), getClass(),
				args[0], IOInfo.CACHE_COL_MATRIX);
		
		Job job2 = new Job(confCol);
		//job2.addDependingJob(job1);
		jc.addJob(job2);
		
		/*** read input and create normalized matrix-row major ****/
		JobConf confRow = RowNormMatrix.getJobConfig(getConf(), getClass(),
				args[0], IOInfo.CACHE_ROW_MATRIX);
		
		Job job1 = new Job(confRow);
		jc.addJob(job1);

	

		/** initialize U and V ***/
//		JobConf jcInitialU = UInitialize.getJobConfig(getConf(), getClass(),
//				IOInfo.TRASH, IOInfo.OUTPUT_U_INITIALIZATION);
//		Job job3 = new Job(jcInitialU);
//		jc.addJob(job3);
//
//		JobConf jcInitialV = VInitialize.getJobConfig(getConf(), getClass(),
//				IOInfo.TRASH, IOInfo.OUTPUT_V_INITIALIZATION);
//		Job job4 = new Job(jcInitialV);
//		jc.addJob(job4);

		/** start the Job Execution **/
		Thread runjobc = new Thread(jc);
		runjobc.start();
		while (!jc.allFinished()) {
			System.out
					.println("jobs left="
							+ (jc.getWaitingJobs().size() + jc.getRunningJobs()
									.size()));// do whatever you want; just wait
												// or ask for job information
			Thread.sleep(15000);
		}
		
		} else {


		/*** iterations to update U and V ***/
		int iter = 1;
		while (iter < 30) {
			/*** Update U ***/
			JobConf jcupdateU = JobUpdateU.getJobConfig(getConf(), getClass(),
					IOInfo.OUTPUT_U + (iter - 1), IOInfo.CACHE_ROW_MATRIX,
					IOInfo.OUTPUT_U + iter, IOInfo.OUTPUT_V + (iter - 1),
					iter);
			JobClient.runJob(jcupdateU);

			/*** Update V ***/
			JobConf jcupdateV = JobUpdateV.getJobConfig(getConf(), getClass(),
					IOInfo.CACHE_COL_MATRIX, IOInfo.OUTPUT_V + (iter - 1),
					IOInfo.OUTPUT_V + iter, IOInfo.OUTPUT_U + (iter), 
					iter);
			JobClient.runJob(jcupdateV);

			/*** 
			 * Move the RMSE generated file 
			 * from directory:/std57/output/v_iteri to 
			 * :"/std57/temp/rmse" otherwise it will read in next Iteration
			 ***/
			
			/** RMSE calculation for small dataset **/
			if(CALCULATE_RMSE) {
				FileSystem fs = FileSystem.get(jcupdateV);
				fs.rename(new Path(IOInfo.OUTPUT_V+iter+"/rmse-r-00000"), 
						new Path("/std57/temp/rmse"));
				readRmse(iter, "/std57/temp/rmse", "/std57/rmseresult");
			}
			iter++;
		}
		}
		return 0;
	}
	
	/*
	 * It read the RMSE from the file and sums up the total Rmse value
	 */
	public void readRmse(int iter, String path, String path2) {
		BufferedReader fileReader = null;
		float rmse = 0;
		try {
			fileReader = new BufferedReader(new FileReader(path.toString()));
			String line;
			int sum=0;
			while ((line = fileReader.readLine()) != null) {
				String[] xyz = line.split(",", 2);
				if (!xyz[0].equals("NaN")) {
					String[] tokens = xyz[1].split(":");
					rmse += Float.parseFloat(tokens[0]);
					sum+= Integer.parseInt(tokens[1]);
				}
			}
			rmse = rmse/sum;
			rmse = (float) Math.sqrt(rmse);
			FileWriter fstream = new FileWriter(path2, true);
			fstream.write(iter+"\t"+Float.toString(rmse)+"\n");
			fstream.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fileReader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
