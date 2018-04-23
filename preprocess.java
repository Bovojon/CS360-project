import java.io.IOException;
import org.apache.hadoop.io.Text;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class yell {

    //class to implement the mapper interface.
    static class SumMapper extends Mapper<Object, Text, Text, Text>{
        public boolean isInteger( String input ) {
            try {
                Integer.parseInt( input );
                return true;
            }
            catch( Exception e ) {
                return false;
            }
        }

        //Map interface.
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          
        String[] strTk;
        StringTokenizer itr = new StringTokenizer(value.toString());
        
		while (itr.hasMoreTokens()) {
            //splitting using ","
            strTk = itr.nextToken().split("\",\"");
            if (isInteger(strTk[3])) {
                //[review_id][business_id][stars][reviews]
                context.write(new Text(strTk[0]), new Text(strTk[0] + "|" + strTk[2] + "|" + strTk[3] + "|" + strTk[5])); 
            }    
        }
    }
}
    //class to implement the reducer interface.
    static class SumReducer extends Reducer<Text, Text, Text, Text> {

        //Reduce interface.
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {            
        String[] strTk;
        String[] split;
        String join;
		for(Text value : values) {

            strTk = value.toString().split("|");

            //alternate way.
            
            /*
            //Splitting the review using \n as delimiter.
            split=strTk[3].split("\n")
            //joining the array.
            join = String.join(" ", split);
            */
      
            strTK[3] = strTk[3].replace("\n","");
            strTK[3] = strTk[3].replace("\"","");

            context.write(new Text("output"), new Text(strTk[0] + "|" + strTk[1] + "|" + strTk[2] + "|" + strTk[3]));
        
        }
    }
}

    //class to implement the driver interface.
    static class SumDriver extends Configured implements Tool {
        public int run(String[] arg0) throws Exception {
            Configuration conf =  new Configuration();
            //conf.set("review.average.business.id","BuffallowWildWings");
            conf.set("mapreduce.output.textoutputformat.separator", ",");
            Job job = Job.getInstance(conf,"review Average");
            
            //Set different classes
            job.setJarByClass(SumDriver.class);
            job.setMapperClass(SumMapper.class);
            job.setCombinerClass(SumReducer.class);
            job.setReducerClass(SumReducer.class);
            
            //Set output parameters
            FileInputFormat.addInputPath(job, new Path(arg0[0]));
            FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }
        
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SumDriver(), args);
    }
}
	

