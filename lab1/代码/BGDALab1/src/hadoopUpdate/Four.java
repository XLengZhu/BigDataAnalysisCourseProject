package hadoopUpdate;

import hadoop.Record;
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
import java.util.HashMap;

public class Four{
    public static Record incomeRecord = new Record();
    public static Record ratingRecord = new Record();
    public static HashMap<String,Record> ratingCount = new HashMap<>();
    public static HashMap<String,Record> incomeCount = new HashMap<>();
    public static final class DefaultFillMapper extends Mapper<LongWritable, Text, Text, Text>{
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] list = line.split("\\|");
            if (!list[11].contains("?")){
                String nationality_career = list[10]+list[11];
                double income = Double.parseDouble(list[11]);
                incomeRecord.setNum(incomeRecord.getNum()+1);
                incomeRecord.setSum(incomeRecord.getSum()+income);
                if (!incomeCount.containsKey(nationality_career)){
                    Record record = new Record();
                    record.setSum(income);
                    record.setNum(1);
                    incomeCount.put(nationality_career,record);
                }
                else {
                    Record record = incomeCount.get(nationality_career);
                    record.setSum(record.getSum()+income);
                    record.setNum(record.getNum()+1);
                }
            }
            if (!list[6].contains("?")){
                String longitude_latitude_altitude = list[1]+list[2]+list[3];
                double rating = Double.parseDouble(list[6]);
                ratingRecord.setNum(ratingRecord.getNum()+1);
                ratingRecord.setSum(ratingRecord.getSum()+rating);
                if (!ratingCount.containsKey(longitude_latitude_altitude)){
                    Record record = new Record();
                    record.setSum(rating);
                    record.setNum(1);
                    ratingCount.put(longitude_latitude_altitude,record);
                }
                else {
                    Record record = ratingCount.get(longitude_latitude_altitude);
                    record.setSum(record.getSum()+rating);
                    record.setNum(record.getNum()+1);
                }
            }
            context.write(value,new Text(""));
        }
    }
    public static final class DefaultReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String line = key.toString();
            String[] list = line.split("\\|");
            if (list[11].contains("?")){
                String nationality_career = list[10] + list[11];
                if (incomeCount.containsKey(nationality_career)){
                    Record record = incomeCount.get(nationality_career);
                    double income = record.getSum()/ record.getNum();
                    line = line.replace(list[11], String.valueOf(income));
                }else {
                    double income = incomeRecord.getSum()/incomeRecord.getNum();
                    line = line.replace(list[11], String.valueOf(income));
                }
            }
            if (list[6].contains("?")){
                String longitude_latitude_altitude = list[1]+list[2]+list[3];
                if (ratingCount.containsKey(longitude_latitude_altitude)){
                    Record record = ratingCount.get(longitude_latitude_altitude);
                    double rating = record.getSum() / record.getNum();
                    line = line.replace(list[6], String.valueOf(rating));
                }else {
                    double rating = ratingRecord.getSum()/ratingRecord.getNum();
                    line = line.replace(list[6], String.valueOf(rating));
                }
            }
            key = new Text(line);
            for (Text value:values){
                context.write(key, new Text(""));
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        String input = "hdfs://localhost:9000/D_OneToThree/part-r-00000";
        String out = "hdfs://localhost:9000/D_Four";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Four");
        job.setJarByClass(Four.class);
        job.setMapperClass(DefaultFillMapper.class);
        job.setReducerClass(DefaultReducer.class);

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
