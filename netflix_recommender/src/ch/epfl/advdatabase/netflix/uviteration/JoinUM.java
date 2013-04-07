package ch.epfl.advdatabase.netflix.uviteration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import ch.epfl.advdatabase.netflix.setting.Constants;
import ch.epfl.advdatabase.netflix.setting.IOInfo;

public class JoinUM {
	
	public static JobConf getJobConfig(Configuration con, Class cla, String output) throws IOException {
		JobConf conf = new JobConf(con, cla);
		conf.setJobName("join UM");
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		//conf.setInputFormat(KeyValueTextInputFormat.class);
		
		conf.setReducerClass(UMReducer.class);
		conf.setNumReduceTasks(Constants.U_FILES);
		conf.set("mapred.textoutputformat.separator",":");
		
		//create empty file
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);
		
		//Specifying the input directories(@ runtime) and Mappers independently for inputs from multiple sources
        MultipleInputs.addInputPath(conf, new Path(IOInfo.OUTPUT_U_INITIALIZATION), TextInputFormat.class, UReadMapper.class);
        MultipleInputs.addInputPath(conf, new Path(IOInfo.TEMP_USER_OUTPUT), TextInputFormat.class, MreadUmapper.class);

        FileOutputFormat.setOutputPath(conf, new Path(output));
		
		return conf;
	}
}
