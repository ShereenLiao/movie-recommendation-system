import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 按照推荐得分降序排序，每个用户列出10个推荐物品
 *
 */
public class Step6 {
    private final static Text K = new Text();
    private final static Text V = new Text();

    public static void main(String[] args) {
        try {
            Job job = Job.getInstance();
            job.setJobName("step6");
            job.setJarByClass(Step6.class);
            job.setMapperClass(Step6_Mapper.class);
            job.setReducerClass(Step6_Reducer.class);
//            job.setSortComparatorClass(NumSort.class);
//            job.setGroupingComparatorClass(UserGroup.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

//            FileInputFormat.addInputPath(job, new Path(paths.get("Step6Input")));
//            Path outpath = new Path(paths.get("Step6Output"));
//            if (fs.exists(outpath)) {
//                fs.delete(outpath, true);
//            }
//            FileOutputFormat.setOutputPath(job, outpath);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Step6_Mapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            String u = tokens[0];
            String item = tokens[1];
            String num = tokens[2];
            K.set(u);
            V.set(item+","+num);
            context.write(K, V);
        }
    }

    // 注意，排序是按照用户id和num排序，分组是按照用户id排序
    static class Step6_Reducer extends Reducer<Text, Text, Text, Text> {
        private final int RECOMMENDER_NUM = 10;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<Tup> movieList = new ArrayList<>();
            for (Text line : values) {
                String[] tokens = Pattern.compile("[\t,]").split(line.toString());
                if (tokens.length >= 2)
                {
                    movieList.add(new Tup(tokens[0], Double.parseDouble(tokens[1])));
                }
            }
                        //然后通过比较器来实现排序
            Collections.sort(movieList, (a, b) -> (int)(b.rating - a.rating));

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < RECOMMENDER_NUM && i < movieList.size(); i++)
            {
                if (movieList.get(i).rating > new Double(0.001))
                {
                    sb.append("," + movieList.get(i).movieId + ":" + String.format("%.2f", movieList.get(i).rating));
                }
            }
            String result = sb.toString();
            if(result.equals("")){
                context.write(key, new Text("none"));
            }else{
                context.write(key, new Text(result.substring(1)));
            }
        }
    }
    static class Tup{
        public String movieId;
        public double rating;
        public Tup(String movieId, double rating){
            this.movieId = movieId;
            this.rating = rating;
        }
    }
}