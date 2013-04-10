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

import ch.epfl.advadb.Main;
import ch.epfl.advadb.IO.TupleTriplet;
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
public class UpdateVReducer extends MapReduceBase implements Reducer<IntWritable, TupleTriplet, IntWritable, Text>{
	
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
					System.out.println("vreducer");
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
	 
	List<Integer> userIds ;
	List<Float> ratings ;
	List<Float> productUV ;
	
	float[] vFeature = new float[10]; //0 won't be used
	
	@Override
	public void reduce(IntWritable key, Iterator<TupleTriplet> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		//parse movie ratings and uf
//		if(key.get()>87) {
//			System.out.println("dkjalk");
//		}
//		if(key.get()>100) {
//			System.out.println("we have an error");
//		}
		
		userIds = new ArrayList<Integer>();
		ratings = new ArrayList<Float>();
		productUV = new ArrayList<Float>();
		
		//we will have only two type of values:: either user-rating pair tuples or 
		//Feature vector tuple for particular movieID
		
		while(values.hasNext()) {
			TupleTriplet triple = values.next();
			char type = triple.getFirst();

			switch(type){
			    case 'M':
			        userIds.add(triple.getSecond());
			        ratings.add(triple.getThird());
			        productUV.add(0f);
			        break;
			    case 'V':
			    	vFeature[triple.getSecond()-1] = triple.getThird();
			        break;
			}
		}
		
		//productUV = new ArrayList<Float>(userIds.size());
		
		for(int j=0; j< userIds.size(); j++) {
			int uid = userIds.get(j);
			float sum =0;
			for(int i=0; i<Constants.D; i++) {
				sum+=vFeature[i]*uFeature[uid][i];
			}
			productUV.set(j, sum);
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
			for(int j=0; j<userIds.size(); j++) {
				int uid = userIds.get(j);
				float subtract = ratings.get(j) - productUV.get(j)
						+ vFeature[i]*uFeature[uid][i];
				innProduct += subtract*uFeature[uid][i];
				ujSquare += uFeature[uid][i]*uFeature[uid][i];
			}
			float upFeature =innProduct/ujSquare;
			for(int j=0; j< userIds.size(); j++) {
				int uid = userIds.get(j);
				productUV.set(j, productUV.get(j) + 
						uFeature[uid][i] *(upFeature-vFeature[i]));
				//	It is equivalent to following of two:
					//1. productUV[j] -= uFeature[i]*vFeature[j][i];//old feature contribution subtracted
					//2. productUV[j]  += upFeature*vFeature[j][i];//updated feature contribution added
			}
			vFeature[i]=upFeature;
		}
		
		
		for(int i=0; i< Constants.D; i++) {			
			mos.getCollector("V", reporter).collect(new Text("V,"+(i+1)), new Text(key+","+vFeature[i]));
		}
		if(Main.CALCULATE_RMSE) {
		float rmse= (float) 0.00;
		for(int i =0; i< userIds.size(); i++) {
			int uid = userIds.get(i);
			rmse+=Math.pow(ratings.get(i)-
					productUV.get(i), 2);
		}
		if(userIds.size()>0) {
			mos.getCollector("rmse", reporter).collect(key, new Text(Float.toString(rmse)+":"+userIds.size()));
		}
		}
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		mos.close();
	}
}
