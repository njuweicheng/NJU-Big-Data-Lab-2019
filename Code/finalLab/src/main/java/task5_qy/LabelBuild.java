package task5_qy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LabelBuild {
    //random give each a label
    public static class BuildMapper extends Mapper<Object,Text,Text, Text>
    {
        public void map(Object key, Text value, Context context)
                throws IOException,InterruptedException
        {
            //read <name> - <link>
            String [] strs = value.toString().split("\t");
            String name = strs[0];
            //output <name>-<name>  label is a name
            context.write(new Text(name),new Text(name));
        }
    }

    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "buildLabel");

        job.setJarByClass(LabelBuild.class); //Jar
        job.setInputFormatClass(TextInputFormat.class);  //input format

        job.setMapperClass(BuildMapper.class); //my mapper

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class); //Text is a type like String in JAVA
        job.setOutputValueClass(Text.class); //Text is a type like String in JAVA , show the output is String

        FileInputFormat.addInputPath(job, new Path(args[0])); //input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //output path

        job.waitForCompletion(true); //end
    }
}
