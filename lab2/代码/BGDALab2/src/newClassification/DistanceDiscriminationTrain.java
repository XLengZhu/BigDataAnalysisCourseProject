package newClassification;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistanceDiscriminationTrain {
    public static int dim = 20;
    public static final class DDTrainingMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
            String[] item = value.toString().split(",");
            String type = item[dim];
            String abString = item[0];
            for (int i=1; i<dim; i++){
                abString = abString + "," + item[i];
            }
            context.write(new Text(type), new Text(abString));
        }
    }
    public static final class DDTrainingReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[] center = new double[dim];
            List<double[]> list = new ArrayList<double[]>();
            for (Text item : values){
                double[] temp = textToList(item);
                list.add(temp);
            }
            int number = list.size();
            for (int i=0; i<dim; i++){
                double sum = 0;
                for (double[] temp : list){
                    sum += temp[i];
                }
                center[i] = sum /number;
            }
            String stringResult = String.valueOf(center[0]);
            for (int i=1; i<dim; i++){
                stringResult = stringResult + "," + String.valueOf(center[i]);
            }
            System.out.println("center: "+stringResult);
            context.write(new Text(stringResult),new Text(""));
        }
    }

    public static double[] textToList(Text text){
        double[] list = new double[dim];
        String[] words = text.toString().split(",");
        for (int i=0; i< words.length; i++){
            list[i] = Double.parseDouble(words[i]);
        }
        return list;
    }

    public static void main(String[] arg) throws Exception{

        Path inputpath=new Path("hdfs://localhost:9000/lab2/训练数据.txt");
        Path outputpath=new Path("hdfs://localhost:9000/lab2/Distance_Discrimination_Train_result");
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf, "DDTraining");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setJarByClass(DistanceDiscriminationTrain.class);
        job.setMapperClass(DDTrainingMapper.class);
        job.setReducerClass(DDTrainingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
