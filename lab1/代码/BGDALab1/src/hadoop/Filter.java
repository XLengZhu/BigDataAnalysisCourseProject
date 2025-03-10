package hadoop;
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
public class Filter {
    public static final class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] list = line.split("\\|");
            double longitude = Double.parseDouble(list[1]);
            double latitude = Double.parseDouble(list[2]);
            if (longitude >= 8.1461259 && longitude < 11.1993265 && latitude >= 56.5824865 && latitude <= 57.750511) {
                context.write(value, new Text(""));
            }
        }
    }
    public static final class FilterReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
            context.write(key, new Text(""));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        String input = "hdfs://localhost:9000/D_Sample/part-r-00000";
        String out = "hdfs://localhost:9000/D_Filter";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Filter");
        job.setJarByClass(Filter.class);
        job.setMapperClass(FilterMapper.class);

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









