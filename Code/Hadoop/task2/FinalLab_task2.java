package task2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class FinalLab_task2 {
    public static class Mapper_task2 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text namePair = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] names = value.toString().split(" ");

            HashSet<String> nameSet = new HashSet<String>(Arrays.asList(names));
            String[] noRepeatNames = new String[nameSet.size()];
            nameSet.toArray(noRepeatNames);

            //System.out.println(Arrays.toString(noRepeatNames));

            for (int i = 0; i < noRepeatNames.length; i++) {
                for (int j = 0; j < noRepeatNames.length; j++) {
                    if (i!=j) {
                        namePair.set("<" + noRepeatNames[i] + "," + noRepeatNames[j] + ">");
                        context.write(namePair, one);
                    }
                }
            }
        }
    }


    public static class Combiner_task2 extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static class Partitioner_task2 extends HashPartitioner<Text, IntWritable>{

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String firstName = key.toString().split(",")[0].substring(1);   // <name1,name2> => name1
            return super.getPartition(new Text(firstName), value, numReduceTasks);
        }
    }

    public static class Reducer_task2 extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task2_CoexistenceStatistic");

        job.setJarByClass(FinalLab_task2.class);
        job.setMapperClass(Mapper_task2.class);
        job.setCombinerClass(Combiner_task2.class);
        job.setPartitionerClass(Partitioner_task2.class);
        job.setReducerClass(Reducer_task2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        // System.exit(job.waitForCompletion(true)? 0 : 1);

    }
}
