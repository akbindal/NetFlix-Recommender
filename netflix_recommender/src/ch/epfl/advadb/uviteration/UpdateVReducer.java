package ch.epfl.advadb.uviteration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

import ch.epfl.advadb.setting.Constants;

/**
 * 
 * This class represents the reduce phase which is responsible for new feature values of movieIds (V) by
 * of the reducing phase of V and M join, where V is the feature vector of movie Id
 * and M is the col-major matrix where Rows=>UserIds and Columns=>MovieIds. It uses distributed cache to load
 * the U matrix in memory by overriding the Configure function so this class loads the U matrix while setting up
 * the map task at node and makes it accessible to use. 
 * 
 * OUTPUT
 * 1. This writes out the updated feature values of movieId which is rows of V matrix
 * 
 * INPUT
 * 1. Input of the V is taken from the map phase represented by class:VReadMapper which has 
 * <key, <value>>: <movieId, <V:i:fv>*> where fv is the value of ith feature for movieId , 10 values for feature
 * 2. Input of the M is taken from the map phase represented by class:MReadVMapper which has
 * <key, <value>>: <movieId, <M:userId1,rat1:userId2,rat2:userId3,rat3...>> , 1 user-rating pairs tuple
 * According to the Project specification, we will have in total 10 feature value
 * therefore Total values for each key in this reduce phase needs to be exactly 11: 
 * (10 tuple of feature value) + 1 user-rating Pairs
 * 
 * Distributed Cache:
 * It loads the U matrix by reading the files of U matrix from previous iteration, It uses loadUMatrix function 
 * to read it.
 * 
 * @author ashish
 *
 */
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
				String[] tokens = line.split(",");
				int userid = Integer.parseInt(tokens[1]);
				String featureIndex = tokens[2]; // = tokens[1].split(",");
				try{
				int fi = Integer.parseInt(featureIndex);
				String featureValue = tokens[3];
				float fv = Float.parseFloat(featureValue);
				uFeature[userid][fi-1]=fv;
				} catch (Exception e) {
					System.out.println("kljlkj");
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
	
	float[] vFeature = new float[10]; //0 won't be used
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		//parse movie ratings and uf
//		if(key.get()>87) {
//			System.out.println("dkjalk");
//		}
		
		String userRatPairs="";
		String vfeature="";
		
		//we will have only two values:: either user-rating pair tuples or 
		//Feature vector tuple for particular movieID
		
		while(values.hasNext()) {
			String line = values.next().toString();
			String[] tokens = line.split(":", 2);
			
			
			String tupleType = tokens[0];
			if(tupleType.equals("M")) {
				userRatPairs = tokens[1];
			} else if(tupleType.equals("V")){
				String[] pair = tokens[1].split(":");
				vFeature [Integer.parseInt(pair[0])-1]=  Float.parseFloat(pair[1]);//tokens[1];
//				if(vfeature.length()<20) {
//					System.out.println("kjlk");
//				}
			}
		}
		
		//parse movieId from movieRatinPairs
		String[] tokens = userRatPairs.split(":");
	
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
				// if user doesn't have any movie 
			}
		}

		String[] features = vfeature.split(",");
		
//		try {
//			for(int i=0; i<Constants.D; i++) {
//				vFeature[i] = Float.parseFloat(features[i]);
//			}
//		} catch (Exception e) {
//			System.out.println("VException=>"+features.toString());
//		}
		
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
		List<Integer> featureIndex = new ArrayList<Integer>(10);
		for(int i=0; i<Constants.D; i++) featureIndex.add(i);
		Collections.shuffle(featureIndex);
		
		for(int x: featureIndex) {
			int i = featureIndex.get(x); //for(int i=0; i<Constants.D; i++) {
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
				productUV[j] = productUV[j] + uFeature[uid][i] *(upFeature-vFeature[i]);
				//	It is equivalent to following of two:
					//1. productUV[j] -= uFeature[i]*vFeature[j][i];//old feature contribution subtracted
					//2. productUV[j]  += upFeature*vFeature[j][i];//updated feature contribution added
			}
			vFeature[i]=upFeature;
		}
		
		for(int i=0; i< Constants.D; i++) {			
			mos.getCollector("V", reporter).collect(new Text("V,"+(i+1)), new Text(key+","+vFeature[i]));
		}
		
		float rmse= (float) 0.00;
		for(int i =0; i< userIds.length; i++) {
			int uid = userIds[i];
			rmse+=Math.pow(ratings[i]-productUV[i], 2);
		}
		if(userIds.length>0) {
			mos.getCollector("rmse", reporter).collect(key, new Text(Float.toString(rmse)+":"+userIds.length));
		}
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		mos.close();
	}
}
