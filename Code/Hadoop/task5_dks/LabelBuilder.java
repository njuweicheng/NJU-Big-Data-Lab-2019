package task5_dks;

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

public class LabelBuilder {
    static Integer labelCount = 0;


    public static class Mapper_LabelBuilder extends Mapper<LongWritable, Text, Text, Text>{

        Text sendText1 = new Text();
        Text sendText2 = new Text();
        //IntWritable sendInt = new IntWritable();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String name = value.toString().split("\t")[0];
            labelCount++;
            sendText1.set(name);
            sendText2.set("#" + labelCount.toString());
            context.write(sendText1, sendText2);
        }
    }

    public static class Reducer_LabelBuilder extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            super.reduce(key, values, context);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task5_LabelBuilder");

        job.setJarByClass(LabelBuilder.class);
        job.setMapperClass(Mapper_LabelBuilder.class);
        job.setReducerClass(Reducer_LabelBuilder.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
