package task4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class RankViewer {
    // RankViewer Mapper
    public static class RankViewerMapper extends Mapper<Text, Text, FloatWritable, Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String rankInfo = value.toString().split("#")[0];
            context.write(new FloatWritable(Float.parseFloat(rankInfo)), key);
        }
    }

    // sort in descending order
    public static class FloatWritableDecreasingComparator extends FloatWritable.Comparator{
        @Override
        public int compare(Object a, Object b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class RankViewerReducer extends Reducer<FloatWritable, Text, Text, FloatWritable>{
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values){
                context.write(value, key);
            }
        }
    }

    public static void main(String inPath, String outPath) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRank Viewer");

        job.setJarByClass(RankViewer.class);
        job.setMapperClass(RankViewerMapper.class);
        job.setReducerClass(RankViewerReducer.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setSortComparatorClass(FloatWritableDecreasingComparator.class);

        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
    }
}
