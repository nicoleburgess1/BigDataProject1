/*
Identify "outdated" LinkBookPage. Return IDs and nicknames of persons that have not
accessed LinkBook for 90 days (i.e., no entries in the AccessLog in the last 90 days)
*/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskG {

    public static class TokenizerMapper
            extends Mapper<Object, Text, String, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text Education = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            String dataset = value.toString();
            String[] datapoint = dataset.split("/n");
            String[][] columns = new String[datapoint.length][];
            for(int i = 0; i < columns.length; i++){
                columns[i] = datapoint[i].split(",");
            }


            for (String[] column : columns) {
                Integer intCol = Integer.parseInt(column[4]);
                if (intCol<129600){
                    context.write(column[1], one);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<String,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {


        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Education Count");
        job.setJarByClass(TaskG.class);
        job.setMapperClass(TaskG.TokenizerMapper.class);
        job.setNumReduceTasks(0);
        //job.setCombinerClass(TaskG.IntSumReducer.class);
        //job.setReducerClass(TaskG.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("TestaccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("TaskGOutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
