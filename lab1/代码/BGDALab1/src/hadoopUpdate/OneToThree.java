package hadoopUpdate;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class OneToThree {
    public static double max = Double.NEGATIVE_INFINITY;
    public static double min = Double.POSITIVE_INFINITY;

    //FormatNormalize
    public static String DateFormatConversion(String s) throws ParseException {
        SimpleDateFormat format1 = new SimpleDateFormat("MMMM d,yyyy", Locale.ENGLISH);
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat format3 = new SimpleDateFormat("yyyy/MM/dd");
        if (s.contains(",")){
            Date date = format1.parse(s);
            return format3.format(date);
        }
        else if (s.contains("-")){
            Date date = format2.parse(s);
            return format3.format(date);
        }
        else {
            return s;
        }
    }
    public static class SampleMapper extends Mapper<LongWritable, Text, Text ,Text> {
        protected void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException {
            String line = value.toString();
            String[] list = line.split("\\|");
            //FormatNormalize
            if (!list[6].equals("?")){
                double rating = Double.parseDouble(list[6]);
                if (rating > max){
                    max = rating;
                }
                if (rating < min){
                    min = rating;
                }
            }
            //Sample
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
                    String line = item.toString();
                    String[] list = line.split("\\|");
                    double longitude = Double.parseDouble(list[1]);
                    double latitude = Double.parseDouble(list[2]);
                    //Sample
                    if (longitude >= 8.1461259 && longitude < 11.1993265 && latitude >= 56.5824865 && latitude <= 57.750511){
                        //FormatNormalize
                        if (!list[6].equals("?")){
                            double rating = Double.parseDouble(list[6]);
                            rating = (rating-min)/(max-min);
                            line = line.replace(list[6], String.valueOf(rating));
                        }
                        //temperature
                        if (list[5].contains("℉")){
                            float temperature = Float.parseFloat(list[5].substring(0,list[5].length()-1));
                            temperature = (temperature-32)/1.8f;
                            String t = String.format("%.1f",temperature)+"℃";
                            line = line.replace(list[5],t);
                        }
                        //date
                        try {
                            String review_date = DateFormatConversion(list[4]);
                            String user_birthday = DateFormatConversion(list[8]);
                            line = line.replace(list[4],review_date);
                            line = line.replace(list[8],user_birthday);
                        }catch (ParseException e){
                            e.printStackTrace();
                        }
                        context.write(new Text(line),new Text(""));
                    }
                }
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        String input = "hdfs://localhost:9000/analyze/data.txt";
        String out = "hdfs://localhost:9000/D_OneToThree";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"OneToThree");
        job.setJarByClass(OneToThree.class);
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
