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

public class TaskB {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

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
                word.set(column[2]);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static class SortMapper
            extends Mapper<Text, IntWritable, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public void map(Text key, IntWritable value, Mapper.Context context
        ) throws IOException, InterruptedException {

            String dataset = value.toString();
            String[] datapoint = dataset.split("/n");
            String[][] columns = new String[datapoint.length][];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = datapoint[i].split(",");
            }

            for (String[] column : columns) {
                word.set(column[1]);
                context.write(word, column[0]);
            }

        }
    }





    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapperClass(SortMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        //job1
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Page Access Count");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapperClass(SortMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("TestaccessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("TaskBOutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //job 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "sort and get max 10");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(TokenizerMapper.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(IntSumReducer.class);

        job2.setMapperClass(SortMapper.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("TaskBOutput/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path("TaskBJob2Output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        //job 2
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "join");
        job3.setJarByClass(TaskB.class);
        job3.setMapperClass(TokenizerMapper.class);
        job3.setCombinerClass(IntSumReducer.class);
        job3.setReducerClass(IntSumReducer.class);

        job3.setMapperClass(SortMapper.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path("TaskBJob2Output/part-r-00000"));
        FileOutputFormat.setOutputPath(job3, new Path("TaskBJob3Output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}