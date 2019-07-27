package task1;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.nlpcn.commons.lang.tire.domain.Forest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class Preprocessing {
    // Mapper
    public static class PreprocessingMapper extends Mapper<Object, Text, NullWritable, Text>{
        String dictKey = "dictKey";
        private ArrayList<String> nameList = new ArrayList<String>();
        private MultipleOutputs<NullWritable, Text> multipleOutputs = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();

            DicLibrary.put(dictKey, dictKey, new Forest());
            String line;
            BufferedReader dataReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
            // define dictionary
            while ((line = dataReader.readLine()) != null){
                line = line.trim();
                DicLibrary.insert(dictKey, line, "name", 1000);
            }

            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName().split("\\.")[0];
            // participle
            Result parse = DicAnalysis.parse(value.toString(), DicLibrary.get(dictKey));
            for (Term term : parse) {
                if (term.getNatureStr().equals("name")){
                    nameList.add(term.getName());
                }
            }

            if (!nameList.isEmpty()) {
                // sort
                Collections.sort(nameList);
                String result = "";
                for (String temp : nameList) {
                    result += temp + " ";
                }
                result = result.trim();
                nameList.clear();

                multipleOutputs.write(NullWritable.get(), new Text(result), generateFileName(fileName));
            }
        }

        private String generateFileName(String value){
            return value+".segmented";
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Task 1");
        job.addCacheFile(new Path(args[0]).toUri());

        job.setJarByClass(Preprocessing.class);

        job.setMapperClass(PreprocessingMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        job.waitForCompletion(true);
        // System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
