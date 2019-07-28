package task4;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class PageRankIter {
    private static final NumberFormat numberFormat = new DecimalFormat("0.0000");

    // PageRankIter Mapper
    public static class PageRankIterMapper extends Mapper<Text, Text, Text, Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] infoList = value.toString().split("#");
            context.write(key, new Text(infoList[1]));  // <key, link_list>

            float preRank = Float.parseFloat(infoList[0]);
            for (String linkInfo : infoList[1].split("\\|")) {
                String[] linkInfoList = linkInfo.split(",");
                float ratio = Float.parseFloat(linkInfoList[1]);
                context.write(new Text(linkInfoList[0]), new Text(numberFormat.format(preRank * ratio))); // <key, rank_inc>
            }

        }
    }

    // PageRankIter Combiner
    public static class PageRankIterCombiner extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (Text value : values){
                String info = value.toString();
                if (info.split(",").length > 1){    // <key, link_list>
                    context.write(key, value);
                }
                else{   // <key, rank_inc>
                    sum += Float.parseFloat(info);
                }
            }
            if (sum > 0){
                context.write(key, new Text(numberFormat.format(sum)));
            }
        }
    }

    // PageRankIter Reducer
    public static class PageRankIterReducer extends Reducer<Text, Text, Text, Text>{
        private final float d = 0.85f;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            String linkList = null;
            for (Text value : values){
                String info = value.toString();
                if (info.split(",").length > 1){    // <key, link_list>
                    linkList = info;
                }
                else{   // <key, rank_inc>
                    sum += Float.parseFloat(info);
                }
            }

            float newPageRank = 1-d+d*sum;
            context.write(key, new Text(numberFormat.format(newPageRank)+"#"+linkList));
        }
    }

    public static void main(String inPath, String outPath) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRank Iter");

        job.setJarByClass(PageRankIter.class);
        job.setMapperClass(PageRankIterMapper.class);
        job.setCombinerClass(PageRankIterCombiner.class);
        job.setReducerClass(PageRankIterReducer.class);

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
