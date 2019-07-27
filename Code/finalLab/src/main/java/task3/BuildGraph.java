package task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;


public class BuildGraph {

    //my mapper
    public static class buildGraphMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //every value is <name,name> number
            //so first split
            String temp = value.toString();//get string
            String[] strings = temp.split("\t");//get split by space, 0
            String people_names = strings[0];
            String count = strings[1];
            people_names  = people_names.replace("<","");
            people_names  = people_names.replace(">",""); //replace < >
            Text key_out = new Text(people_names);
            IntWritable value_out = new IntWritable(); // people,people --> count
            value_out.set(new Integer(count));
            context.write(key_out,value_out); //write
        }
    }


    //my combiner
    public static class buildGraphCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        //combiner
        //set dict to count numbers
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException,InterruptedException{
            int sum = 0;
            for (IntWritable val:values) { //
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }


    public static class buildGraphPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String token = key.toString().split(",")[0];    // key: (token,filename) --> (token) ,make sure to same reducer
            return super.getPartition(new Text(token), value, numReduceTasks);
        }
    }


    //my reducer
    public static class buildGraphReducer extends Reducer<Text, IntWritable, Text, Text> {

        private DecimalFormat decimalFormat = new DecimalFormat("0.0000");    // keep two decimals
        Integer sum = 0;
        Text outputKey = new Text(" ");
        Text currentItem = new Text("");

        HashMap<String, Integer> key_values = new HashMap<String, Integer>();

        @Override
        public void reduce(Text key,Iterable<IntWritable> values, Context context)
                throws IOException,InterruptedException {
            //count sum
            //output
            String[] StringArray = key.toString().split(","); //set
            currentItem = new Text(StringArray[0]);
            if (outputKey.toString().equals(" ")){
                outputKey.set(currentItem.toString()); //review
            }
            else if (!currentItem.equals(outputKey)) {
                boolean isFirst = true;
                StringBuilder out = new StringBuilder(); //output
                out.append("[");
                for (String kk : key_values.keySet()) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        out.append("|");
                    }
                    String output_count = kk + "," + decimalFormat.format(key_values.get(kk) / (double) sum);
                    out.append(output_count);
                }
                out.append("]");
                //shuchu
                sum = 0;
                key_values.clear(); //clear
                context.write(outputKey, new Text(out.toString())); // people->[people,count|...// ]
                outputKey.set(currentItem.toString()); //review
            }
            int temp_sum = 0; //sum of one word
            for (IntWritable val : values) {
                temp_sum += val.get();
            }
            sum += temp_sum;
            key_values.put(StringArray[1], temp_sum);
        }

        // the last token will not be record, fix the mistake
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //output
            boolean isFirst = true;
            StringBuilder out = new StringBuilder(); //output
            out.append("[");
            for (String kk : key_values.keySet()) {
                if(isFirst){
                    isFirst = false;
                }
                else{
                    out.append("|");
                }
                String output_count = kk + ","+ decimalFormat.format(key_values.get(kk) / (double)sum);
                out.append(output_count);
            }
            out.append("]");
            //shuchu
            context.write(outputKey,new Text(out.toString())); // people->[people,count|...// ]
        }
    }




        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        try {
            Configuration conf = new Configuration();

            //compute the numbers of Filesystem
            Path inPath = new Path(args[0]);
            FileSystem fileSystem = FileSystem.get(conf);
            int fileNum = fileSystem.listStatus(inPath).length;
            conf.setInt("Filenumber", fileNum);

            Job job = Job.getInstance(conf, "buildGraph");

            job.setJarByClass(BuildGraph.class); //Jar
            job.setInputFormatClass(TextInputFormat.class);  //input format

            job.setMapperClass(buildGraphMapper.class); //my mapper
            job.setCombinerClass(buildGraphCombiner.class); //my combiner
            job.setPartitionerClass(buildGraphPartitioner.class); //my partioner
            job.setReducerClass(buildGraphReducer.class); //my reducer

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class); //Text is a type like String in JAVA
            job.setOutputValueClass(Text.class); //Text is a type like String in JAVA , show the output is String

            FileInputFormat.addInputPath(job, new Path(args[0])); //input path
            FileOutputFormat.setOutputPath(job, new Path(args[1])); //output path

            job.waitForCompletion(true);
            // System.exit(job.waitForCompletion(true) ? 0 : 1); //end
        } catch (
                Exception e) {
            e.printStackTrace();
        }
    }
}
