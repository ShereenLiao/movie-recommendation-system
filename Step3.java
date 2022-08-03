import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对物品组合列表进行计数，建立物品的同现矩阵
 * i100:i100	3
 * i100:i105	1
 * i100:i106	1
 * i100:i109	1
 * i100:i114	1
 * i100:i124	1
 *
 */
public class Step3 {
    private final static Text K = new Text();
    private final static IntWritable V = new IntWritable(1);

    public static void main(String[] args){
        try {
            Job job =Job.getInstance();
            job.setJobName("step3");
            job.setJarByClass(Step3.class);
            job.setMapperClass(Step3_Mapper.class);
            job.setReducerClass(Step3_Reducer.class);
            job.setCombinerClass(Step3_Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

//            FileInputFormat.addInputPath(job, new Path(paths.get("Step3Input")));
//            Path outpath=new Path(paths.get("Step3Output"));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

//            job.setNumReduceTasks(5);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String[] items = tokens[1].split(",");
            //构成同现矩阵
            for (int i = 0; i < items.length; i++) {
                String itemA = items[i].split(":")[0];
                for (int j = 0; j < items.length; j++) {
                    String itemB = items[j].split(":")[0];
                    K.set(itemA + ":" + itemB);
                    context.write(K, V);
                }
            }
        }
    }

    static class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> i,
                              Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : i) {
                sum = sum + v.get();
            }
            V.set(sum);
            context.write(key, V);
        }
    }
}