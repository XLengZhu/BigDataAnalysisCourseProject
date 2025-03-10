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
public class EQInOneYear {

    public static final class InOneYearMapper extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] list = line.split("\t");
            String data = list[1].split(" ")[0];
            String year = data.substring(0,4);
            if (year.equals("2022")){
                context.write(new Text(list[7]),new Text("1"));
            }
        }
    }
    public static final class InOneYearReducer extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (Text item : values){
                num++;
            }
            context.write(key,new Text(String.valueOf(num)));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String input = "hdfs://localhost:9000/final_work/dataNew.txt";
        String out = "hdfs://localhost:9000/final_work/EQIn2022";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"EQInOneYear");
        job.setJarByClass(EarthquakeCount.class);
        job.setMapperClass(InOneYearMapper.class);
        job.setReducerClass(InOneYearReducer.class);

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
