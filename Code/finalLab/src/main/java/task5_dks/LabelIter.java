package task5_dks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class LabelIter {

    // global variables will be unaccessible when running in distributed hardware
    //static HashMap<String, Integer> nameLabels = new HashMap<String, Integer>();

    public static class Mapper_LabelIter extends Mapper<LongWritable, Text, Text, Text>{

        //Text sendText1 = new Text();
        //Text sendText2 = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] strings = value.toString().split("\t");
            String host = strings[0];
            String[] neighbors = strings[1].substring(1,strings[1].length()-1).split("\\|");

            for(int i = 0; i < neighbors.length; i++){
                context.write(new Text(host), new Text(neighbors[i]));  // key = host, value = (neighborName, weight)
            }
        }
    }


    public static class Reducer_LabelIter extends Reducer<Text, Text, Text, Text> {
        //HashMap<Integer, Float> weightMap = new HashMap<Integer, Float>();
        HashMap<String, Integer> nameLabels = new HashMap<String, Integer>();

        protected void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fileList = fs.listStatus(new Path(conf.get("readLastIterResult")));

            BufferedReader in = null;
            FSDataInputStream fsDataInputStream = null;
            String line = null;
            for(int i = 0; i < fileList.length; i++){
                if(!fileList[i].isDirectory()){
                    fsDataInputStream = fs.open(fileList[i].getPath());
                    in = new BufferedReader(new InputStreamReader(fsDataInputStream, "UTF-8"));
                    while((line = in.readLine()) != null){
                        String[] strings = line.split("\t");
                        nameLabels.put(strings[0], Integer.parseInt(strings[1].substring(1)));
                    }
                }
            }
            if(fsDataInputStream != null) fsDataInputStream.close();
            if(in != null) in.close();
        }
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            //System.out.print(key.toString() + " ");
            HashMap<Integer, Float> weightMap = new HashMap<Integer, Float>();
            for(Text value: values) {
                //System.out.print(value.toString() + " ");

                String[] strings = value.toString().split(",");
                Integer type = nameLabels.get(strings[0]);
                Float weight = Float.parseFloat(strings[1]);
                if (weightMap.containsKey(type)) {
                    Float tmp = weightMap.get(type);
                    weightMap.put(type, tmp + weight);
                } else {
                    weightMap.put(type, weight);
                }
                //nameLabels.put(key.toString(), type);       // renew nameLabels
                //context.write(key, new Text("#" + type.toString()));
            }
            //System.out.println("");

            List<Map.Entry<Integer, Float>> list = new ArrayList<Map.Entry<Integer, Float>>(weightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<Integer, Float>>() {
                public int compare(Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2) {
                    return -o1.getValue().compareTo(o2.getValue());
                }
            });
            for(Map.Entry<Integer, Float> mapping: list){
                nameLabels.put(key.toString(), mapping.getKey());       // renew nameLabels
                //System.out.println("LabelIter Reduce Emit: key = " + key.toString() + " value = " + mapping.getKey().toString());
                context.write(key, new Text("#" + mapping.getKey().toString()));
                break;
            }

        }

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("readLastIterResult", args[1]);

        Job job = Job.getInstance(conf, "task5_LabelIter");

        job.setJarByClass(LabelIter.class);
        job.setMapperClass(Mapper_LabelIter.class);
        //job.setCombinerClass(Combiner_LabelIter.class);
        job.setReducerClass(Reducer_LabelIter.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // graph info
        //FileInputFormat.addInputPath(job, new Path(args[1]));   // input label info
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // output label info

        //System.exit(job.waitForCompletion(true)? 0 : 1);
        job.waitForCompletion(true);

    }
}
