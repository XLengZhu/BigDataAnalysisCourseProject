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

public class DataCleaning {
    public static final class CleaningMapper extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] list = line.split("\t");
            context.write(new Text(list[2]),new Text(list[3]));
        }
    }
    public static final class CleaningReducer extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text item : values){
                context.write(key, item);
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = "hdfs://localhost:9000/final_work/dataNew.txt";
        String out = "hdfs://localhost:9000/final_work/Cleaning";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"DataCleaning");
        job.setJarByClass(DataCleaning.class);
        job.setMapperClass(CleaningMapper.class);
        job.setReducerClass(CleaningReducer.class);

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
