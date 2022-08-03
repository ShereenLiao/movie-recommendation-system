import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step1 {
    public static void main(String[] args) {
        try {
            Job job = Job.getInstance();
            job.setJobName("step1");
            job.setJarByClass(Step1.class);
            job.setMapperClass(Step1_Mapper.class);
            job.setReducerClass(Step1_Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
//
//            FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
//            Path outpath=new Path(paths.get("Step1Output"));
//            FileOutputFormat.setOutputPath(job, outpath);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setNumReduceTasks(5);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    static class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            if(key.get() != 0){
                context.write(value, NullWritable.get());
            }
        }
    }

    static class Step1_Reducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        protected void reduce(Text key, Iterable<IntWritable> i,
                              Context context)
                throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
}