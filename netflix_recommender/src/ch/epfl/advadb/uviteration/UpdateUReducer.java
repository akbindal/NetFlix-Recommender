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

import ch.epfl.advadb.IO.TupleTriplet;
import ch.epfl.advadb.setting.Constants;

public class UpdateUReducer  extends MapReduceBase implements Reducer<IntWritable, TupleTriplet, Text, Text>  {
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
	
	//<V,fi,movieid,value>
	public void loadVMatrix(Path  path){
		BufferedReader fileReader=null;
		try {
			fileReader= new BufferedReader(
			        new FileReader(path.toString()));
			String line;
			while ((line = fileReader.readLine()) != null) {
				String[] tokens = line.split(",");
				int movieid = Integer.parseInt(tokens[2]);
				String featureIndex = tokens[1]; // = tokens[1].split(",");
				int fi = Integer.parseInt(featureIndex);
				String featureValue = tokens[3];
				try{
					float fv = Float.parseFloat(featureValue);
					vFeature[movieid][fi-1]=fv;
				} catch(Exception e) {
					System.out.println("kljlk");
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
	 
	List<Integer> movieIds ;
	List<Float> ratings ;
	List<Float> productUV ;
	
	float[] uFeature = new float[10]; //0th index is not used
	
	@Override
	public void reduce(IntWritable key, Iterator<TupleTriplet> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		//parse movie ratings and uf
		
		movieIds = new ArrayList<Integer>();
		ratings = new ArrayList<Float>();
		productUV = new ArrayList<Float>();
		
		while(values.hasNext()) {
			TupleTriplet triple = values.next();
			char type = triple.getFirst();

			switch(type){
			    case 'M':
			        movieIds.add(triple.getSecond());
			        ratings.add(triple.getThird());
			        productUV.add(0f);
			        break;
			    case 'U':
			        uFeature[triple.getSecond()-1] = triple.getThird();
			        break;
			}
		}
		
		//
		
		//compute the updated ufeature now
		//calculate productUV
		
		for(int j=0; j< movieIds.size(); j++) {
			int mid = movieIds.get(j);
			float sum =0;
			for(int i=0; i<Constants.D; i++) {
				sum+=uFeature[i]*vFeature[mid][i];
			}
			productUV.set(j, sum);
		}
		
		//for each user feature update
		
		List<Integer> featureIndex = new ArrayList<Integer>(10);
		for(int i=0; i<Constants.D; i++) featureIndex.add(i);
		Collections.shuffle(featureIndex);
		
		for(int x: featureIndex) {
			int i = featureIndex.get(x); //for(int i=0; i<Constants.D; i++) {
			//for each movieId 
			float innProduct=0;
			float viSquare = 0;
			for(int j=0; j<movieIds.size(); j++) {
				int mid = movieIds.get(j);
				float subtract = ratings.get(j)-productUV.get(j)
						+ uFeature[i]*vFeature[mid][i];
				innProduct += subtract*vFeature[mid][i];
				viSquare += vFeature[mid][i]*vFeature[mid][i];
			}
			float upFeature =innProduct/viSquare;
			for(int j=0; j< movieIds.size(); j++) {
				int mid = movieIds.get(j);
				//productUV[j] -= uFeature[i]*vFeature[j][i];//old feature contribution subtracted
				//productUV[j]  += upFeature*vFeature[j][i];//updated feature contribution added
				productUV.set(j, productUV.get(j) + 
						vFeature[mid][i] *(upFeature-uFeature[i])  );
//				if(productUV[j]==Float.NaN ||Float.isInfinite(productUV[j])) {
//					System.out.println("kjdlkjal");
//				}
			}
			if(upFeature==Float.NaN || Float.isInfinite(upFeature)) {
				System.out.println("kdjl");
			}
			uFeature[i]=upFeature;
		}
		Text outputkey = new Text("U,"+key);
		for(int i=0; i< Constants.D; i++) {
			output.collect(outputkey, new Text((i+1)+","+uFeature[i]));
		}
	}
}
