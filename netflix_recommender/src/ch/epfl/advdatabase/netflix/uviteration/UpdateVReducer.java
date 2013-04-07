package ch.epfl.advdatabase.netflix.uviteration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import ch.epfl.advdatabase.netflix.setting.Constants;

public class UpdateVReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
	float[][] uFeature = new float[Constants.NO_USER+1][10];
	private MultipleOutputs mos; //rmse
	@Override
	public void configure(JobConf job)  {
		
		Path[] cacheFiles;
		mos = new MultipleOutputs(job);
		try {
			cacheFiles = DistributedCache.getLocalCacheFiles(job);

			if (null != cacheFiles && cacheFiles.length > 0) {
		    	for (Path cachePath : cacheFiles) {
		    		System.out.println(cachePath.toString());
		    		loadUMatrix(cachePath);
		        }
		    }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	
	//read u file 
	public void loadUMatrix(Path  path){
		BufferedReader fileReader=null;
		try {
			fileReader= new BufferedReader(
			        new FileReader(path.toString()));
			String line;
			while ((line = fileReader.readLine()) != null) {
				String[] tokens = line.split(":", 2);
				int userid = Integer.parseInt(tokens[0]);
				String[] features = tokens[1].split(",");
				int i=0;
				for(String fi: features) {
					float fet = Float.parseFloat(fi);
					uFeature[userid][i]=fet;
					i++;
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
	}
	 
	int[] userIds ;
	float[] ratings ;
	float[] productUV ;
	
	float[] vFeature = new float[10];
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		//parse movie ratings and uf
		if(key.get()>87) {
			System.out.println("dkjalk");
		}
		
		String userRatPairs="";
		String vfeature="";
		//we will have only two values
		while(values.hasNext()) {
			String line = values.next().toString();
			String[] tokens = line.split(":", 2);
			//first is movie id
			String tupleType = tokens[0];
			if(tupleType.equals("M")) {
				userRatPairs = tokens[1];
			} else if(tupleType.equals("V")){
				vfeature= tokens[1];
			}
		}
		
		//parse movieId from movieRatinPairs
		String[] tokens = userRatPairs.split(":");
		//StringTokenizer itr = new StringTokenizer(movieRatPairs,":");
	
		userIds = new int[tokens.length];
		ratings = new float[tokens.length];
		productUV = new float[tokens.length];;
		for(int i =0 ; i< tokens.length; i++) {
			String[] pairs = tokens[i].split(",");
			try {
			int uid = Integer.parseInt(pairs[0]);
			
			userIds[i]= uid;
			float rat = Float.parseFloat(pairs[1]);
			ratings[i] =rat;
			} catch (Exception e) {
				// if user doesn't have any movie System.out.println("jkljk");
			}
		}
//		
//		while(itr.hasMoreTokens()) {
//			String tempToken[] = itr.nextToken().split(",", 2);
//			String movieId = tempToken[0];
//			int mid = Integer.parseInt(movieId);
//			movieIds.add(mid);
//			float rat = Float.parseFloat(tempToken[1]);
//			ratings.add(rat);
//		}
//		
		
		
//		//parse userfeature from ufeature
		String[] features = vfeature.split(",");
		
		try {
		for(int i=0; i<Constants.D; i++) {
			vFeature[i] = Float.parseFloat(features[i]);
		}
		} catch (Exception e) {
			System.out.println("VException=>"+features.toString());
		}
		//compute the updated ufeature now
		//calculate productUV
		
		for(int j=0; j< userIds.length; j++) {
			int uid = userIds[j];
			float sum =0;
			for(int i=0; i<Constants.D; i++) {
				sum+=vFeature[i]*uFeature[uid][i];
			}
			productUV[j]=sum;
		}
		
		//for each user feature update
		String stVFeature = "";
		for(int i=0; i<Constants.D; i++) {
			//for each movieId 
			float innProduct=0;
			float ujSquare = 0;
			for(int j=0; j<userIds.length; j++) {
				int uid = userIds[j];
				float subtract = ratings[j]-productUV[j]
						+ vFeature[i]*uFeature[uid][i];
				innProduct += subtract*uFeature[uid][i];
				ujSquare += uFeature[uid][i]*uFeature[uid][i];
			}
			float upFeature =innProduct/ujSquare;
			for(int j=0; j< userIds.length; j++) {
				int uid = userIds[j];
//				productUV[j] -= uFeature[i]*vFeature[j][i];//old feature contribution subtracted
	//			productUV[j]  += upFeature*vFeature[j][i];//updated feature contribution added
				productUV[j] = productUV[j] + uFeature[uid][i] *(upFeature-vFeature[i]);
			}
			vFeature[i]=upFeature;
			stVFeature += Float.toString(vFeature[i]) +",";
		}
		stVFeature = stVFeature.substring(0, stVFeature.length()-1);
		//String stUFeature = uFeature.toString();
		//stUFeature = stUFeature.substring(1, stUFeature.length() - 1).replace(", ", ",");
		Text value = new Text(stVFeature);
		//rmseoutput.collect(key, new Text(value));
		
		float rmse= (float) 0.00;
		for(int i =0; i< userIds.length; i++) {
			int uid = userIds[i];
			rmse+=ratings[i]-productUV[i];
		}
		mos.getCollector("V", reporter).collect(key, new Text(value));
		mos.getCollector("rmse", reporter).collect(key, new Text(Float.toString(rmse)));
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		mos.close();
	}
}
