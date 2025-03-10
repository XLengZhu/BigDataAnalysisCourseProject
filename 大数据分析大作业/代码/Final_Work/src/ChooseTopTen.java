import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

public class ChooseTopTen {
    public static double max = Double.NEGATIVE_INFINITY;
    public static TopTen topTen = new TopTen();
    public static final class TopTenMapper extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] list = line.split("\t");
            topTen.add(new DataPoint(line,Float.parseFloat(list[6])));
            context.write(new Text(""),new Text("1"));
        }
    }
    public static final class TopTenReducer extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> result = topTen.getTopTen();
            for(String db : result){
                String k = db.split(": ")[0];
                context.write(new Text(k),new Text(""));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = "hdfs://localhost:9000/final_work/dataNew.txt";
        String out = "hdfs://localhost:9000/final_work/TopTen";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"TopTen");
        job.setJarByClass(ChooseTopTen.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(input);
        Path outputPath = new Path(out);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);
    }
}
