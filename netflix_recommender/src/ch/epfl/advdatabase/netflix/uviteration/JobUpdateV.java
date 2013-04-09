package ch.epfl.advdatabase.netflix.uviteration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import ch.epfl.advdatabase.netflix.setting.Constants;

public class JobUpdateV {
	public static JobConf getJobConfig(Configuration con, Class cla, String inColMatrix, String inVMatrix, String output, String cachePath) throws IOException {
		JobConf conf = new JobConf(con, cla);
		conf.setJobName("update v");
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		
		conf.setReducerClass(UpdateVReducer.class);
		//conf.setNumMapTasks(Constants.U_FILES);
		conf.setNumReduceTasks(Constants.U_FILES);
		conf.set("mapred.textoutputformat.separator",":");
		
		//create empty file
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);
		FileStatus[] filestatus = fs.listStatus(new Path(cachePath));
		for (FileStatus status : filestatus) {
		    DistributedCache.addFileToClassPath(status.getPath(), conf);
		}
		//Specifying the input directories(@ runtime) and Mappers independently for inputs from multiple sources
        MultipleInputs.addInputPath(conf, new Path(inVMatrix), TextInputFormat.class, VReadMapper.class);
        MultipleInputs.addInputPath(conf, new Path(inColMatrix), TextInputFormat.class, MreadVmapper.class);
        
        FileOutputFormat.setOutputPath(conf, new Path(output));
        ///rmse
        MultipleOutputs.addNamedOutput(conf, "rmse", TextOutputFormat.class, IntWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(conf, "V", TextOutputFormat.class, IntWritable.class, Text.class);
        
		
		return conf;
	}
}
