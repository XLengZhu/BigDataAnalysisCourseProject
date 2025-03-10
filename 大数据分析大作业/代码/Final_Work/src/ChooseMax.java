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

public class ChooseMax {
    public static double max = Double.NEGATIVE_INFINITY;
    public static String maxLine = "";
    public static boolean flag = false;
    public static final class ChooseMaxMapper extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] list = line.split("\t");
            if (flag){
                double num = Double.parseDouble(list[6]);
                if (num > max){
                    max = num;
                    maxLine = line;
                }
                context.write(new Text(""),value);
            }
            flag = true;
        }
    }
    public static final class ChooseMaxReducer extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(maxLine),new Text(""));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = "hdfs://localhost:9000/final_work/data.txt";
        String out = "hdfs://localhost:9000/final_work/ChooseMax";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"ChooseMax");
        job.setJarByClass(ChooseMax.class);
        job.setMapperClass(ChooseMaxMapper.class);
        job.setReducerClass(ChooseMaxReducer.class);

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
        System.out.println(maxLine);
    }

}
