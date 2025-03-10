package newCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

public class KMeansTwoPlus {

    public static int k = 3;
    public static int dim = 20;
    //    public static List<double[]> lists;
    public static double Max = 0;
    public static double[] largestNode;
    static String DATA_PATH = "hdfs://localhost:9000/lab2/聚类数据.txt";
    static String CENTER_PATH = "hdfs://localhost:9000/lab2/centerKPlus.txt";
    static String NEW_CENTER_PATH = "hdfs://localhost:9000/lab2/KMeansPlus_centers";
    public static final class KMPlusMapper extends Mapper<Object, Text, Text, Text>{
        public static List<double[]> lists = new ArrayList<double[]>();
        protected void setup(Context context) throws IOException {
//            先让List不为空;
//            载入Lists;
            lists = getCenters(CENTER_PATH);
        }
        public void map(Object Key, Text value, Context context) throws IOException,InterruptedException {
            double[] data = textToList(value);
            double cost = 0;
            for (double[] item : lists) {
                cost += getCost(item, data);
            }
            if (cost > Max){
                Max = cost;
                largestNode = data;
            }
            context.write(new Text(" "),new Text("1"));
        }
    }
    public static final class KMPlusReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = String.valueOf(largestNode[0]);
            for (int i=1; i<dim; i++){
                result += ","+ String.valueOf(largestNode[i]);
            }
            System.out.println("The center is "+result);
            context.write(new Text(result), new Text(""));
        }
    }
    public static double getCost(double[] oldone, double[] newone){
        double cost = 0;
        for (int i=0; i<dim; i++){
            cost += Math.pow(Math.abs(oldone[i]-newone[i]),2);
        }
        return cost;
    }
    public static void deleteFile(String pathstr) throws IOException{
        Configuration conf = new Configuration();
        Path path = new Path(pathstr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path,true);
    }
    public static void replaceCenterFile(String centerPath, String newCenterPath) throws IOException{
        Configuration conf = new Configuration();
        Path centerpath = new Path(centerPath);
        FileSystem fileSystem = centerpath.getFileSystem(conf);
        Path newPath = new Path(newCenterPath);
        FileStatus[] listFiles = fileSystem.listStatus(newPath);
        for (int i=0; i<listFiles.length; i++){
            if (listFiles[i].getPath().toString().contains("part")){
                FSDataOutputStream out = fileSystem.append(centerpath);
                FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
//                out.writeBytes("\n");
                IOUtils.copyBytes(in, out, 4096, true);
            }
        }
        deleteFile(newCenterPath);
    }
    public static List<double[]> getCenters(String centerPath) throws IOException{
        List<double[]> centers = new ArrayList<double[]>();
        Path centerpath = new Path(centerPath);
        Configuration conf = new Configuration();
        FileSystem fileSystem = centerpath.getFileSystem(conf);
        FSDataInputStream fsis = fileSystem.open(centerpath);
        LineReader lineReader = new LineReader(fsis, conf);
        Text line = new Text();
        while(lineReader.readLine(line)>0){
            String sk = line.toString();
            if(!sk.isEmpty()){
                double[] list = textToList(line);
                centers.add(list);
            }
        }
        lineReader.close();
        return centers;
    }
    public static double[] textToList(Text text){
        double[] list = new double[dim];
        String line = text.toString();
        if (line.contains("\t")){
            line = line.split("\t")[0];
        }
        String[] words = line.split(",");
        for (int i=0; i< words.length; i++){
            list[i] = Double.parseDouble(words[i]);
        }
        return list;
    }
    public static void run() throws IOException, ClassNotFoundException,InterruptedException{
        Path outputpath = new Path(NEW_CENTER_PATH);
        Path inputpath = new Path(DATA_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeansTwoPlus");
        job.setJarByClass(KMeansTwoPlus.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(KMPlusMapper.class);
        job.setReducerClass(KMPlusReducer.class);

        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        System.out.println(job.waitForCompletion(true));
    }
    public static void main(String[] arg) throws Exception{
        int count = 1;
        while(count < k) {
            run();
            replaceCenterFile(CENTER_PATH,NEW_CENTER_PATH);
            count++;
        }
    }
}