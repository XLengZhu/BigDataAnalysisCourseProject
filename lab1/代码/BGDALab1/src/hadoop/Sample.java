package hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

public class Sample {
    public static class SampleMapper extends Mapper<LongWritable, Text, Text ,Text> {
        protected void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException {
            String line = value.toString();
            String[] list = line.split("\\|");
            Text career = new Text();
            career.set(list[10]);
            context.write(career,value);
        }
    }
    public static final class SampleReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            for (Text item:values){
                int num = (int)(Math.random()*100);
                if(num<10){
                    context.write(item,new Text(""));
                }
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        String input = "hdfs://localhost:9000/analyze/data.txt";
        String out = "hdfs://localhost:9000/D_Sample";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Sample");
        job.setJarByClass(Sample.class);
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(input);
        Path outputPath = new Path(out);
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);
    }
}
