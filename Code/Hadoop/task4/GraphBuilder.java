package task4;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GraphBuilder {
    // GraphBuilder Mapper
    public static class GraphBuilderMapper extends Mapper<Text, Text, Text, Text>{
        private final String initRank = "1.0000";
        private String linkList;

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String info = value.toString();
            linkList = info.substring(1, info.length() - 1); // remove []
            context.write(key, new Text(initRank + "#" + linkList));
        }
    }

    public static void main(String inPath, String outPath) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Graph Builder");

        job.setJarByClass(GraphBuilder.class);
        job.setMapperClass(GraphBuilderMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
    }
}
