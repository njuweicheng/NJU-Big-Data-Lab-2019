package task6_dks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LabelAnalysis {

    public static class Mapper_LabelAnalysis extends Mapper<LongWritable, Text, IntWritable, Text>{

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            int typeIndex = Integer.parseInt(strings[1].substring(1));
            String characterName = strings[0];
            context.write(new IntWritable(typeIndex), new Text(characterName));
            //System.out.println(typeIndex + " " + characterName);
        }
    }


    public static class Reducer_LabelAnalysis extends Reducer<IntWritable, Text, IntWritable, Text>{

        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuilder stringBuilder = new StringBuilder();
            for(Text value: values){
                stringBuilder.append(value.toString() + "|");
            }
            String result = stringBuilder.substring(0,stringBuilder.length()-1);
            context.write(key, new Text(result));
            System.out.println(key.get() + ": " + result);

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task6_LabelAnalysis");
        job.setJarByClass(LabelAnalysis.class);
        job.setMapperClass(Mapper_LabelAnalysis.class);
        job.setReducerClass(Reducer_LabelAnalysis.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
