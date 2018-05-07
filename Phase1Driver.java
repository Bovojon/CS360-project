// CS360 Group Project
// Jon Abdulloev, James Tran, Aakarsh Sinha, Minh Vu

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

public class Phase1Driver extends Configured implements Tool {
	public int run(String[] arg0) throws Exception {
		Configuration conf =  new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		conf.set("textinputformat.record.delimiter", Character.toString((char)13));

		Job job = Job.getInstance(conf,"phase 1");


		// set different classes
		job.setJarByClass(Phase1Driver.class);
		job.setMapperClass(Phase1Mapper.class);
		job.setReducerClass(Phase1Reducer.class);
		
		// set output parameters
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Phase1Driver(), args);
	}
}
