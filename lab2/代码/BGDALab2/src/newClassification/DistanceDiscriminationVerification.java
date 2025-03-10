package newClassification;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class DistanceDiscriminationVerification {
    public static int k = 2;
    public static int dim = 20;
    public static int right = 0;
    public static int sum = 0;
    public static final class DDClassificationMapper extends Mapper<Object, Text, Text, Text> {

        public static double[][] centers = new double[k][dim];
        protected void setup(Context context) throws IOException{
            centers = getCenters("hdfs://localhost:9000/lab2/Distance_Discrimination_Train_result",true);
        }
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
            double[] data = textToList(value);
            double min = Double.POSITIVE_INFINITY;
            int type = 2;
            for (int i=0; i<k; i++){
                double temp = getCost(data, centers[i]);
//                System.out.println(temp);
                if (min > temp){
                    type = i;
                    min = temp;
                }
            }
            sum++;
            String trueValue = value.toString().split(",")[dim];
            if (Integer.parseInt(trueValue)==type){
                right++;
            }
            context.write(new Text(String.valueOf(type)),new Text(trueValue));
        }
        protected void cleanup(Context context)  throws IOException {
            double accuracy = (double)right / (double)sum;
            System.out.println("准确率为：" + accuracy);

        }
    }
    public static double getCost(double[] oldOne, double[] newOne){
        double cost = 0;
        for (int i=0; i<dim; i++){
            cost += Math.pow(Math.abs(oldOne[i] - newOne[i]),2);
        }
        return cost;
    }
    public static double[][] getCenters(String centerPath, boolean inDirectory) throws IOException{
        double[][] centers = new double[k][dim];
        Path centerpath = new Path(centerPath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = centerpath.getFileSystem(conf);
        if (inDirectory){
            FileStatus[] listStatus = fileSystem.listStatus(centerpath);
            for (int i=0; i<listStatus.length; i++){
                if(listStatus[i].getPath().toString().contains("part")){
                    centers = getCenters(listStatus[i].getPath().toString(), false);
                }
            }
        }
        else {
            int i = 0;
            FSDataInputStream fsis = fileSystem.open(centerpath);
            LineReader lineReader = new LineReader(fsis, conf);
            Text line = new Text();
            while(lineReader.readLine(line)>0){
                double[] list = textToList(line);
                centers[i] = list;
                i++;
            }
            lineReader.close();
        }
        return centers;
    }

    public static double[] textToList(Text text){
        double[] list = new double[dim];
        String line = text.toString();
        if (line.contains("\t")){
            line = line.split("\t")[0];
        }
        String[] words = line.split(",");
        for (int i=0; i< dim; i++){
            list[i] = Double.parseDouble(words[i]);
        }
        return list;
    }

    public static final class DDClassificationReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            int sum = 0;
//            int equal = 0;
//            int real = Integer.parseInt(key.toString());
//            for (Text item : values){
//                int train = Integer.parseInt(item.toString());
//                sum++;
//                if (real==train){
//                    equal++;
//                }
//            }
//            double result = (double)equal/(double)sum;
//            System.out.println("The accuracy is"+result);
        }
    }
    public static void main(String[] arg) throws Exception{

        Path outputpath=new Path("hdfs://localhost:9000/lab2/Distance_Discrimination_Classification_Verification");
        Path inputpath=new Path("hdfs://localhost:9000/lab2/验证数据.txt");
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf, "DDClassificationVerification");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setJarByClass(DistanceDiscriminationClassification.class);
        job.setMapperClass(DDClassificationMapper.class);
//        job.setReducerClass(DDClassificationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
