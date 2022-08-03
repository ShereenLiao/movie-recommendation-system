import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 按用户分组，计算所有物品出现的组合列表，得到用户对物品的喜爱度得分矩阵
 * u13 i160:1, u14 i25:1,i223:1, u16 i252:1,
 * u21 i266:1,
 * u24 i64:1,i218:1,i185:1,
 * u26 i276:1,i201:1,i348:1,i321:1,i136:1,
 *
 */
public class Step2 {
    public static void main(String[] args){
        try {
            Job job = Job.getInstance();
            job.setJobName("step2");
            job.setJarByClass(Step2.class);
            job.setMapperClass(Step2_Mapper.class);
            job.setReducerClass(Step2_Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

//            FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
//            Path outpath = new Path(paths.get("Step2Output"));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setNumReduceTasks(5);
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        // 如果使用：用户+物品，同时作为输出key，更优
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String user = tokens[0];
            String item = tokens[1];
            String rating = tokens[2];
            Text k = new Text(user);
            Text v = new Text(item + ":" + rating);
            context.write(k, v);
        }
    }

    static class Step2_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> i, Context context) throws IOException, InterruptedException {
            // 存放商品的名称和喜爱度
            Map<String, Float> r = new HashMap<>();
            for (Text value : i) {
                String[] vs = value.toString().split(":");
                String item = vs[0];
                float rating = Float.parseFloat(vs[1]);
                // 查看该商品是否已有喜爱度，有就相加
                r.put(item, r.getOrDefault(item, (float)0) + rating);
            }
            StringBuffer sb = new StringBuffer();
            boolean fir = true;
            for(String rKey: r.keySet()){
                if(fir){
                    sb.append(rKey + ":" + r.get(rKey));
                    fir = false;
                }else{
                    sb.append("," + rKey + ":" + r.get(rKey));
                }

            }
            context.write(key, new Text(sb.toString()));
        }
    }
}