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

import ch.epfl.advadb.setting.Constants;

public class UpdateUReducer  extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>  {
	float[][] vFeature = new float[Constants.NO_MOVIES+1][10];
	
	@Override
	public void configure(JobConf job)  {
		
		Path[] cacheFiles;
		try {
			cacheFiles = DistributedCache.getLocalCacheFiles(job);

			if (null != cacheFiles && cacheFiles.length > 0) {
		    	for (Path cachePath : cacheFiles) {
		    		System.out.println(cachePath.toString());
		    		loadVMatrix(cachePath);
		        }
		    }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	
	public void loadVMatrix(Path  path){
		BufferedReader fileReader=null;
		try {
			fileReader= new BufferedReader(
			        new FileReader(path.toString()));
			String line;
			while ((line = fileReader.readLine()) != null) {
				String[] tokens = line.split(":", 2);
				int movieid = Integer.parseInt(tokens[0]);
				String[] features = tokens[1].split(",");
				int i=0;
				for(String fi: features) {
					float fet = Float.parseFloat(fi);
					vFeature[movieid][i]=fet;
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
	 
	int[] movieIds ;
	float[] ratings ;
	float[] productUV ;
	
	float[] uFeature = new float[10];
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
		//parse movie ratings and uf
		String movieRatPairs="";
		String ufeature="";
		//we will have only two values
		while(values.hasNext()) {
			String line = values.next().toString();
			String[] tokens = line.split(":", 2);
			//first is movie id
			String tupleType = tokens[0];
			if(tupleType.equals("M")) {
				movieRatPairs = tokens[1];
			} else if(tupleType.equals("U")){
				ufeature= tokens[1];
			}
		}
		
		//parse movieId from movieRatinPairs
		String[] tokens = movieRatPairs.split(":");
		//StringTokenizer itr = new StringTokenizer(movieRatPairs,":");
	
		movieIds = new int[tokens.length];
		ratings = new float[tokens.length];
		productUV = new float[tokens.length];;
		for(int i =0 ; i< tokens.length; i++) {
			String[] pairs = tokens[i].split(",");
			try {
			int mid = Integer.parseInt(pairs[0]);
			
			movieIds[i]= mid;
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
		String[] features = ufeature.split(",");
		for(int i=0; i<Constants.D; i++) {
			uFeature[i] = Float.parseFloat(features[i]);
		}
		
		//compute the updated ufeature now
		//calculate productUV
		
		for(int j=0; j< movieIds.length; j++) {
			int mid = movieIds[j];
			float sum =0;
			for(int i=0; i<Constants.D; i++) {
				sum+=uFeature[i]*vFeature[mid][i];
			}
			productUV[j]=sum;
		}
		
		//for each user feature update
		String stUFeature = "";
		List<Integer> featureIndex = new ArrayList<Integer>(10);
		for(int i=0; i<Constants.D; i++) featureIndex.add(i);
		Collections.shuffle(featureIndex);
		
		for(int x: featureIndex) {
			int i = featureIndex.get(x); //for(int i=0; i<Constants.D; i++) {
			//for each movieId 
			float innProduct=0;
			float viSquare = 0;
			for(int j=0; j<movieIds.length; j++) {
				int mid = movieIds[j];
				float subtract = ratings[j]-productUV[j]
						+ uFeature[i]*vFeature[mid][i];
				innProduct += subtract*vFeature[mid][i];
				viSquare += vFeature[mid][i]*vFeature[mid][i];
			}
			float upFeature =innProduct/viSquare;
			for(int j=0; j< movieIds.length; j++) {
				int mid = movieIds[j];
				//productUV[j] -= uFeature[i]*vFeature[j][i];//old feature contribution subtracted
				//productUV[j]  += upFeature*vFeature[j][i];//updated feature contribution added
				productUV[j] = productUV[j] + vFeature[mid][i] *(upFeature-uFeature[i]);
//				if(productUV[j]==Float.NaN ||Float.isInfinite(productUV[j])) {
//					System.out.println("kjdlkjal");
//				}
			}
			if(upFeature==Float.NaN || Float.isInfinite(upFeature)) {
				System.out.println("kdjl");
			}
			uFeature[i]=upFeature;
			//stUFeature += Float.toString(uFeature[i]) +",";
		}
		for(int i=0; i< Constants.D; i++) {
			stUFeature += Float.toString(uFeature[i]) +",";
		}
		stUFeature = stUFeature.substring(0, stUFeature.length()-1);
		//String stUFeature = uFeature.toString();
		//stUFeature = stUFeature.substring(1, stUFeature.length() - 1).replace(", ", ",");
		Text value = new Text(stUFeature);
		output.collect(key, new Text(value));
	}
}
