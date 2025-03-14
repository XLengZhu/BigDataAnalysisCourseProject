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
public class Mining {
    static String DATA_PATH = "hdfs://localhost:9000/final_work/Cleaning/part-r-00000";
    static String CENTER_PATH = "hdfs://localhost:9000/final_work/centerForThree.txt";
    static String NEW_CENTER_PATH = "hdfs://localhost:9000/final_work/MiningResultForThree";
    public static int k = 3;		// 分为k个中心
    public static int dim = 2;		// 数据维度为20
    public static final class KMeansMapper extends Mapper<Object, Text, Text, Text>{
        public static double[][] centers = new double[k][dim];
        protected void setup(Context context) throws IOException{
            centers = getCenters(CENTER_PATH,false);
        }
        public void map(Object Key, Text value, Context context) throws IOException,InterruptedException{
            double[] item = textToList(value);
            double minDistance = 9999999;
            int centerIndex = 0;
            for (int i=0; i<k; i++){
                double tempDistance = 0;
                for (int j=0; j<dim; j++){
                    tempDistance += Math.pow(Math.abs(centers[i][j]-item[j]),2);
                }
                if (tempDistance < minDistance){
                    minDistance = tempDistance;
                    centerIndex = i;
                }
            }
            context.write(new Text(String.valueOf(centerIndex)),value);
        }
    }
    public static final class KMeansReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            List<double[]> fieldList = new ArrayList<double[]>();
            for (Text value : values){
                double[] list = textToList(value);
                fieldList.add(list);
            }
            int number = fieldList.size();
            double[] avg = new double[dim];
            for (int i=0; i<dim; i++){
                double sum = 0;
                for (double[] temp : fieldList){
                    sum += temp[i];
                }
                avg[i] = sum/number;
            }
            String resultAvg = String.valueOf(avg[0]);
            for (int i=1; i<dim; i++){
                resultAvg = resultAvg + "\t" + String.valueOf(avg[i]);
            }
            System.out.println("center" + String.valueOf(key) + ": " + resultAvg);
            context.write(new Text(resultAvg), new Text(""));
        }
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
        String[] words = line.split("\t");
        for (int i=0; i< dim; i++){
            list[i] = Double.parseDouble(words[i]);
        }
        return list;
    }
    public static void deleteFile(String pathstr) throws IOException{
        Configuration conf = new Configuration();
        Path path = new Path(pathstr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path,true);
    }
    public static boolean compareCenter(String centerPath, String newCenterPath) throws IOException{
        double[][] oldCenters = getCenters(centerPath, false);
        double[][] newCenters = getCenters(newCenterPath,true);
        for (int i=0; i<k; i++){
            for (int j=0; j<dim; j++){
                if (Math.abs(oldCenters[i][j]-newCenters[i][j]) > 1e-2){
                    replaceCenterFile(centerPath, newCenterPath);
                    return false;
                }
            }
        }
        deleteFile(newCenterPath);
        return true;
    }
    public static void replaceCenterFile(String centerPath, String newCenterPath) throws IOException{
        Configuration conf = new Configuration();
        Path centerpath = new Path(centerPath);
        FileSystem fileSystem = centerpath.getFileSystem(conf);
        FSDataOutputStream overWrite = fileSystem.create(centerpath, true);
        overWrite.writeChars("");
        overWrite.close();
        Path newPath = new Path(newCenterPath);
        FileStatus[] listFiles = fileSystem.listStatus(newPath);
        for (int i=0; i<listFiles.length; i++){
            if (listFiles[i].getPath().toString().contains("part")){
                FSDataOutputStream out = fileSystem.create(centerpath);
                FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
                IOUtils.copyBytes(in, out, 4096, true);
            }
        }
        deleteFile(newCenterPath);
    }
    public static void run(boolean runReduce) throws IOException, ClassNotFoundException,InterruptedException{
        Path outputpath = new Path(NEW_CENTER_PATH);
        Path inputpath = new Path(DATA_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans_Mining");
        job.setJarByClass(Mining.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(KMeansMapper.class);
        if (runReduce){
            job.setReducerClass(KMeansReducer.class);
        }

        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        System.out.println(job.waitForCompletion(true));
    }
    public static void main(String[] arg) throws Exception{
        int count = 0;
        while(true){
            run(true);
            System.out.println("Round "+count+" calculation");
            count++;
            if (compareCenter(CENTER_PATH, NEW_CENTER_PATH)){
                run(false);
                break;
            }
        }
    }
}
