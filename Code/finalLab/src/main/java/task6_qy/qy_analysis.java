package task6_qy;

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

public class qy_analysis{

    public static class Mapper_qyAnalysis extends Mapper<LongWritable, Text, Text, Text>{

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            context.write(new Text(strings[1]), new Text(strings[0]));
        }
    }

    public static class Reducer_qyAnalysis extends Reducer<Text, Text, Text, Text>{

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuilder stringBuilder = new StringBuilder();
            for(Text value: values){
                stringBuilder.append(value.toString() + "|");
            }
            String result = stringBuilder.substring(0,stringBuilder.length()-1);
            context.write(key, new Text(result));

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task6_LabelAnalysis");
        job.setJarByClass(qy_analysis.class);
        job.setMapperClass(Mapper_qyAnalysis.class);
        job.setReducerClass(Reducer_qyAnalysis.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
