package task5_qy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class LpaIter {

    private String readRes = null;
    //random give each a label
    public static class LpaIterMapper extends Mapper<Object,Text,Text,FloatWritable>
    {
        Map<String,String> keyLabel = new HashMap<String,String>(); //read

        @Override
        public void setup(Context context) throws IOException, InterruptedException //readLabel
        {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FileStatus[] fileList = fs.listStatus(new Path(conf.get("readRes")));

            BufferedReader in = null;
            FSDataInputStream fsi = null;
            String line = null;
            for(int i = 0; i < fileList.length;i++){
                if(!fileList[i].isDirectory())
                {
                    fsi = fs.open(fileList[i].getPath());
                    in = new BufferedReader(new InputStreamReader(fsi,"UTF-8"));
                    while((line = in.readLine())!=null){
                        String []temp = line.split("\t");
                        keyLabel.put(temp[0],temp[1]);//end
                    }
                }
            }
            in.close();
            fsi.close();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException,InterruptedException
        {
            String name = value.toString().split("\t")[0];
            String peopleList = value.toString().split("\t")[1];

            peopleList = peopleList.replace("[","");
            peopleList = peopleList.replace("]","");

            String [] plist = peopleList.split("\\|");
            //iter compute
            for(int i = 0;i < plist.length;i++){
                String Neighborname = plist[i].split(",")[0];
                String Neighborlabel = keyLabel.get(Neighborname);
                String weight = plist[i].split(",")[1]; //compute
                FloatWritable w = new FloatWritable(Float.parseFloat(weight));
                context.write(new Text(name+","+Neighborlabel),w);
            }
        }
    }


    public static class LpaIterCombiner extends Reducer<Text,FloatWritable,Text,FloatWritable>
    {
        HashMap<String,String> keyLabel = new HashMap<String,String>(); //read
        //local combiner
        @Override
        public void reduce(Text key, Iterable<FloatWritable> value, Context context)
                throws IOException,InterruptedException
        {
            float sum = 0;
            for(FloatWritable a : value){
                sum += a.get();
            }
            context.write(key,new FloatWritable(sum));
        }
    }

    // Partitioner
    public static class LpaIterPartitioner extends HashPartitioner<Text,FloatWritable> {
        @Override
        public int getPartition(Text key, FloatWritable value, int numReduceTasks) {
            String token = key.toString().split(",")[0];    // key: (token,filename) --> (token)
            return super.getPartition(new Text(token), value, numReduceTasks);
        }
    }


    //random give each a label
    public static class LpaIterReducer extends Reducer<Text,FloatWritable,Text,Text>
    {
        private String curItem;
        private String KeyOutput = "";
        private Map<String,Float> mapKV = new HashMap<String, Float>();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> value, Context context)
                throws IOException,InterruptedException
        {
            //Reduce
            String [] name_label = key.toString().split(",");
            curItem = name_label[0];
            String label = name_label[1];
            if(!curItem.equals(KeyOutput) && !KeyOutput.equals("")){ //not equal and out
                //find max and output the acorrding label
                float val = 0;
                String res = "";
                for(Map.Entry<String, Float> entry: mapKV.entrySet())
                {
                    if(entry.getValue() > val){
                        val = entry.getValue();
                        res = entry.getKey();
                    }
                }
                context.write(new Text(KeyOutput),new Text(res));
                mapKV = new HashMap<String, Float>();
            }
            //compute
            float weight = 0;
            for(FloatWritable val : value){
                weight += val.get();
            }
            //put in dict
            if(mapKV.containsKey(label)){
                mapKV.put(label,weight+mapKV.get(label));
            }
            else{
                mapKV.put(label,weight);
            }
            KeyOutput = curItem;
        }

        @Override
        public void  cleanup(Context context) throws IOException, InterruptedException {
            float val = 0;
            String res = "";
            for(Map.Entry<String, Float> entry: mapKV.entrySet())
            {
                if(entry.getValue() > val){
                    val = entry.getValue();
                    res = entry.getKey();
                }
            }
            context.write(new Text(KeyOutput),new Text(res));
            mapKV = new HashMap<String, Float>();
        }
    }

    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("readRes", args[1]); //read result label

        Job job = Job.getInstance(conf, "buildLabel");
        job.setJarByClass(LpaIter.class); //Jar
        job.setInputFormatClass(TextInputFormat.class);  //input format

        job.setMapperClass(LpaIterMapper.class); //my mapper
        job.setCombinerClass(LpaIterCombiner.class);
        job.setPartitionerClass(LpaIterPartitioner.class);
        job.setReducerClass(LpaIterReducer.class); //my reducer

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(job, new Path(args[2])); //output path

        job.waitForCompletion(true); //end
    }
}
