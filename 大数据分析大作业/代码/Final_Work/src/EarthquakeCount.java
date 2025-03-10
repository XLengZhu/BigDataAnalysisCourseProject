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
public class EarthquakeCount {

    public static final class CountMapper extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] list = line.split("\t");
            context.write(new Text(list[7]),new Text("1"));
        }
    }
    public static final class CountReducer extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (Text item : values){
                num++;
            }
            context.write(key,new Text(String.valueOf(num)));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = "hdfs://localhost:9000/final_work/data.txt";
        String out = "hdfs://localhost:9000/final_work/Count";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Count");
        job.setJarByClass(EarthquakeCount.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

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
